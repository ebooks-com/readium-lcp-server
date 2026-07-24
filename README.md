# Readium LCP Server (eBooks.com fork)

Readium LCP DRM license & status servers, tuned for eBooks.com's SQL Server and high-concurrency stack.

[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)
[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD--3--Clause-blue.svg?style=flat-square)](LICENSE.md)
[![Go 1.24](https://img.shields.io/badge/Go-1.24-00ADD8.svg?style=flat-square)](go.mod)

This is an eBooks.com fork of [`readium/readium-lcp-server`](https://github.com/readium/readium-lcp-server), carrying performance and compatibility changes tuned for eBooks.com's runtime stack. See [Background](#background) for what differs and why.

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Configuration](#configuration)
- [API](#api)
- [Security](#security)
- [Maintainers](#maintainers)
- [Contributing](#contributing)
- [License](#license)

## Background

[Readium LCP](https://readium.org/lcp-specs/) (Licensed Content Protection) is an interoperable DRM for EPUB, PDF, audiobook, and comic publications. This project provides the server-side tools: a **License Server** (`lcpserver`), a **Status Server** (`lsdserver`), and a command-line **encryption tool** (`lcpencrypt`).

### Why this fork exists

eBooks.com runs the LCP servers against Microsoft SQL Server, under high request concurrency, fetching licenses in bulk, for a catalogue that includes large (> 4 GB) publication files. This fork adds changes tuned for that environment. Some are specific enough to eBooks.com's stack that they are unlikely to be wanted upstream.

The `source` git remote tracks `readium/readium-lcp-server`. The fork branched at v1.13.2, keeps its changes on top, and aims to stay mergeable with upstream. General LCP fixes belong upstream; see [Contributing](#contributing).

### What differs from upstream

The fork's distinctive additions are bulk-fetch endpoints and the SQL Server tuning they rely on, both shaped by eBooks.com's access pattern and deployment and not present in upstream:

- **Batch endpoints.** `POST /licenses/batch` and `POST /licenses/batch/status` (see [API](#api)) fetch many licenses or statuses in a single request instead of one call each. Fetching licenses and statuses for 80 books drops from ~160 calls to 2. Batch building is parallelized, and the per-row lookup of `content` rows it would otherwise incur is avoided.
- **SQL Server tuning for batch lookups.** String parameters are sent as `mssql.VarChar` so the server's `varchar` indexes are used, avoiding an implicit per-row `nvarchar`→`varchar` conversion, and batch lookups are chunked to stay within SQL Server's 2100-parameter limit.

It also carries one fix ahead of upstream: file sizes (`Publication.Size` / `LCPServerMsgV2.Size`) are widened from `uint32` to `int64`, removing truncating casts that corrupted size metadata for publications over 4 GB.

Other fixes the fork once carried, such as connection-pool tuning and wider print/copy rights, have since been adopted upstream, so the fork stays thin. General LCP fixes belong upstream; see [Contributing](#contributing).

For LCP concepts, deployment topology, and the full configuration reference, see the [upstream Wiki](https://github.com/readium/readium-lcp-server/wiki).

## Install

Requires a working [Go](https://go.dev/) 1.24 installation.

Build this fork from source:

```sh
git clone https://github.com/ebooks-com/readium-lcp-server.git
cd readium-lcp-server
go build -o "$GOPATH/bin" ./lcpencrypt
go build -o "$GOPATH/bin" ./lcpserver
go build -o "$GOPATH/bin" ./lsdserver
```

> **Note:** `go install github.com/readium/readium-lcp-server/...@latest` installs upstream. The Go module path is still `github.com/readium/readium-lcp-server` (the fork did not rename it), so there is no `@latest` install path for the eBooks.com build. Clone this repository and build from source.

On Alpine Linux, add the `musl` build tag, e.g. `go build -tags musl -o "$GOPATH/bin" ./lcpserver`.

On Windows, install a GCC toolchain (e.g. [TDM-GCC](https://jmeubank.github.io/tdm-gcc/)) so the SQLite module compiles, and use `%GOPATH%` with backslashes in paths.

## Usage

Each server reads a YAML config file located via an environment variable, then runs as a long-lived process:

```sh
export READIUM_LCPSERVER_CONFIG=/usr/local/var/lcp/config.yaml
export READIUM_LSDSERVER_CONFIG=/usr/local/var/lcp/config.yaml
lcpserver &   # License Server: intranet only, reached by your CMS over REST
lsdserver &   # Status Server: public, behind an HTTPS reverse proxy
```

`lcpencrypt` is a one-shot CLI that encrypts a publication and optionally notifies the License Server:

```sh
lcpencrypt -input book.epub -contentid <uuid> -lcpsv http://user:pass@lcp.internal:8989
```

Both servers require an SQL database and a config file; see [Configuration](#configuration).

## Configuration

The full per-property reference lives in the [upstream Wiki](https://github.com/readium/readium-lcp-server/wiki). The essentials, plus the eBooks.com-specific parts:

**Profiles.** `profile: basic` (default) is the open test profile and provides no real security. The production profile requires confidential crypto material and an X.509 certificate issued by [EDRLab](https://www.edrlab.org) (contact@edrlab.org), the LCP Certification Authority.

**Database.** Set the `database` connection string on each server. eBooks.com targets SQL Server:

```yaml
lcp:
    database: "mssql://server={server};user id={user};password={pw};database={db}"
```

SQLite, MySQL, and PostgreSQL are also supported; schema-creation scripts for each are in [`dbmodel/`](dbmodel/). SQLite databases are created automatically on first run; the other engines require applying the matching `dbmodel/` script first. `lcpserver` and `lsdserver` must use separate databases.

**Connection pooling.** Both servers configure database connection-pool defaults (max open, max idle, connection lifetimes) to sustain concurrent load. No configuration is required.

## API

This fork adds two batch endpoints. Each accepts a JSON body of license IDs and returns all results in a single response, with per-item errors for any that fail. One request replaces N round trips. The standard per-license LCP and LSD REST endpoints are unchanged from upstream; see the [Readium LCP specifications](https://readium.org/lcp-specs/) and the [upstream Wiki](https://github.com/readium/readium-lcp-server/wiki).

### `POST /licenses/batch` — License Server

Returns the requested licenses. Requires HTTP Basic authentication, like all License Server endpoints.

```http
POST /licenses/batch
Content-Type: application/json

{ "license_ids": ["<id1>", "<id2>", "..."] }
```

### `POST /licenses/batch/status` — Status Server

Returns the requested license status documents, including expiration checks and status links. Public, like the other status endpoints.

```http
POST /licenses/batch/status
Content-Type: application/json

{ "license_ids": ["<id1>", "<id2>", "..."] }
```

## Security

The default `basic` profile is for testing only and provides no security. Production deployments require the production profile and its EDRLab-issued certificate.

Encrypted publications are safe to serve from public storage (a web-accessible filesystem or an S3 bucket): each publication is encrypted once with a strong cipher, and its decryption key is held separately in the License Server's SQL database. The License Server itself must remain on the intranet, reachable only by your CMS.

## Maintainers

[eBooks.com Engineering](https://github.com/ebooks-com). For questions or problems with this fork, open an issue on [this repository](https://github.com/ebooks-com/readium-lcp-server/issues).

## Contributing

This is an internal eBooks.com fork.

**Please send LCP improvements upstream**, to [`readium/readium-lcp-server`](https://github.com/readium/readium-lcp-server). That benefits every LCP implementer and keeps this fork thin. Only changes genuinely specific to eBooks.com's stack (SQL Server tuning, the bulk-fetch endpoints) belong here; open an issue or PR on [this repository](https://github.com/ebooks-com/readium-lcp-server/issues) for those.

Upstream contributor credits are listed in [CONTRIBUTIONS.md](CONTRIBUTIONS.md).

## License

[BSD-3-Clause](LICENSE.md) © 2017 Readium.