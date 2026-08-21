// Command build compiles the LCP server binaries with cgo enabled and a C
// compiler on PATH, so the module can be built from an editor task on any host.
//
// Usage: go run ./tools/build [lcpserver] [lsdserver] [lcpencrypt]
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

var targets = []string{"lcpserver", "lsdserver", "lcpencrypt"}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "build:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}

	selected, err := selectTargets(args)
	if err != nil {
		return err
	}

	envPath := filepath.Join(root, envFile)
	compiler, fromEnvFile, err := resolveCompiler(readEnvFile(envPath)["GCC_BIN"])
	if err != nil {
		return err
	}
	fmt.Println("build: using the C compiler in", compiler)

	// Delve launches the binary without going through this command, so the
	// resolved directory is recorded for the debugger's environment file.
	if !fromEnvFile {
		if err := writeEnvValue(envPath, "GCC_BIN", compiler); err != nil {
			return err
		}
	}

	for _, target := range selected {
		if err := buildTarget(root, target, compiler); err != nil {
			return err
		}
	}
	return nil
}

func selectTargets(args []string) ([]string, error) {
	if len(args) == 0 {
		return targets, nil
	}
	for _, arg := range args {
		if !slices.Contains(targets, arg) {
			return nil, fmt.Errorf("unknown target %q, expected one of %s", arg, strings.Join(targets, ", "))
		}
	}
	return args, nil
}

func buildTarget(root, target, compiler string) error {
	outputDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return err
	}

	output := filepath.Join(outputDir, target)
	if runtime.GOOS == "windows" {
		output += ".exe"
	}

	cmd := exec.Command("go", "build", "-o", output, "./"+target)
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(),
		"CGO_ENABLED=1",
		"PATH="+compiler+string(os.PathListSeparator)+os.Getenv("PATH"),
	)

	fmt.Println("build:", target)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("building %s: %w", target, err)
	}
	return nil
}

// repoRoot walks up from the working directory to the directory holding go.mod,
// so the command works whatever directory the editor task runs it from.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod found above the working directory")
		}
		dir = parent
	}
}
