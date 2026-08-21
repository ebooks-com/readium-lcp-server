package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

// resolveCompiler returns the directory holding a C compiler, and whether that
// directory came from GCC_BIN rather than from a search.
func resolveCompiler(gccBin string) (dir string, fromEnvFile bool, err error) {
	if gccBin != "" {
		if found, ok := compilerIn(gccBin); ok {
			return found, true, nil
		}
		return "", false, fmt.Errorf("GCC_BIN in %s is %q, which holds no cc or gcc", envFile, gccBin)
	}

	// The UCRT64 directories are searched before PATH because a MSVCRT-based
	// toolchain such as TDM-GCC cannot link go-fitz's prebuilt MuPDF libraries.
	for _, candidate := range preferredDirs() {
		if found, ok := compilerIn(candidate); ok {
			return found, false, nil
		}
	}

	for _, name := range []string{"cc", "gcc"} {
		if path, lookErr := exec.LookPath(name); lookErr == nil {
			return filepath.Dir(path), false, nil
		}
	}

	return "", false, fmt.Errorf("no C compiler found; %s", installHint())
}

// preferredDirs lists the MSYS2 UCRT64 locations, the toolchain this module is
// verified against on Windows. Other hosts are expected to have a system cc.
func preferredDirs() []string {
	if runtime.GOOS != "windows" {
		return nil
	}
	dirs := []string{`C:\msys64\ucrt64\bin`}
	if root := os.Getenv("MSYS2_ROOT"); root != "" {
		dirs = append(dirs, filepath.Join(root, "ucrt64", "bin"))
	}
	return dirs
}

func compilerIn(dir string) (string, bool) {
	for _, name := range []string{"cc", "gcc"} {
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return dir, true
		}
	}
	return "", false
}

func installHint() string {
	switch runtime.GOOS {
	case "windows":
		return "install MSYS2 from https://www.msys2.org/ then run: pacman -S mingw-w64-ucrt-x86_64-gcc"
	case "darwin":
		return "install the Xcode command line tools: xcode-select --install"
	default:
		return "install gcc using your distribution's package manager"
	}
}
