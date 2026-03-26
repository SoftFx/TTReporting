# TTReporting

A collection of scheduled reporting scripts and a web dashboard for TT (Trading Technologies) platform data. Each project is packaged as a self-contained Docker image.

---

## Repository structure

```
TTReporting/
├── source/                  # R source code (one sub-folder per project)
│   ├── common/              # Shared R helpers (PostgresHost.R, RMonitoringClient.R)
│   ├── big-trades/
│   ├── count-trades/
│   ├── diff-prices/
│   ├── big-deposits/
│   ├── negative-equity/
│   └── aggr-data_App/       # Shiny web dashboard
│
├── sourcePython/            # Python source code
│   ├── common/              # Shared Python helpers
│   └── swap-updater/        # Python + .NET project
│
├── build/                   # Dockerfiles (one file per project, no extension)
│   ├── big-trades
│   ├── count-trades
│   ├── diff-prices
│   ├── big-deposits
│   ├── negative-equity
│   ├── swap-updater
│   └── aggr-data_app
│
├── examples/                # Ready-to-use run scripts and docker-compose files
│   ├── docker-compose.yml   # Runs ALL batch projects from Docker Hub images
│   ├── big-trades/
│   ├── count-trades/
│   ├── diff-prices/
│   ├── big-deposits/
│   ├── negative-equity/
│   └── swap-updater/
│
├── build_docker_*.bat       # Local build helper scripts
└── .github/workflows/
    └── release-dockerhub.yml  # CI/CD: builds and pushes images to Docker Hub
```

> **Not tracked by git** (listed in `.gitignore`):
> - `configDocker/` — credentials and per-instance configuration
> - `dataDocker/` — runtime persistence (state, output files, logs)

---

## Project types

### Batch scripts (R / Python)

Projects: `big-trades`, `count-trades`, `diff-prices`, `big-deposits`, `negative-equity`, `swap-updater`

- Run once, do their work, then exit (`restart: "no"` in docker-compose).
- Intended to be triggered on a schedule (cron, Task Scheduler, etc.).
- Each container reads config from `configDocker/` and writes results/state to `dataDocker/`.

**R projects** — entry point is `Rscript /app/main.R`
**swap-updater** — Python + .NET, entry point is `python3 main.py`

### Web dashboard (Shiny)

Project: `aggr-data_App`

- Runs as a persistent web service on port **3838**.
- Provides an interactive UI for querying aggregator LP execution data from PostgreSQL.

---

## Volumes

Every container expects two bind-mounted directories:

| Path inside container | Host path (example)          | Mode | Purpose                   |
|-----------------------|------------------------------|------|---------------------------|
| `/app/configDocker`   | `./big-trades/configDocker`  | ro   | Config files, credentials |
| `/app/dataDocker`     | `./big-trades/dataDocker`    | rw   | Output files, state, logs |

`configDocker/` must be created manually and populated before first run. It is excluded from git to protect credentials.

### Typical configDocker contents (batch R projects)

```
configDocker/
├── config.yaml          # DB connection settings, report parameters
└── symbol_setups.csv    # (project-specific) symbol filter / setup table
```

### Typical dataDocker contents (written by the container)

```
dataDocker/
├── state.json           # Timestamp of last run, run counter
├── last_run.txt
└── task_exec_log.csv    # Execution log per run
```

---

## Shared code

R projects share helpers from `source/common/`. These are copied into the image at `/common/` during the build and sourced at runtime:

```r
source('../common/PostgresHost.R')      # PostgreSQL connection helpers
source('../common/RMonitoringClient.R')
```

Python projects share helpers from `sourcePython/common/`, copied to `/common/` with `PYTHONPATH=/common:/app`.

---

## Building Docker images

### Locally (one project)

Run the `.bat` script from the repo root, or run the command directly:

```bat
docker build -t big-trades -f build/big-trades .
```

The build context is always the **repo root** (`.`), because Dockerfiles reference paths like `source/big-trades/` and `source/common/`.

> `configDocker/` and `dataDocker/` are excluded from the build context via `.dockerignore` — they are never baked into the image.

### CI/CD — GitHub Actions → Docker Hub

Workflow: `.github/workflows/release-dockerhub.yml`

Triggered by:
- A **GitHub Release** being published → builds all projects.
- **Manual dispatch** (`workflow_dispatch`) → choose a single project or `all`.

Published tags on Docker Hub (`softfx/tt-reporting`):

| Tag pattern                       | Example                           |
|-----------------------------------|-----------------------------------|
| `latest-<project>`                | `latest-big-trades`               |
| `<calver>-<project>`              | `2025.06.10-big-trades`           |
| `sha-<short_sha>-<project>`       | `sha-a1b2c3d-big-trades`          |
| `sha256-<short_digest>-<project>` | `sha256-abcdef012345-big-trades`  |

CalVer format: `YYYY.MM.DD` (first build of the day) or `YYYY.MM.DD.N` (subsequent builds on the same day).

---

## Running projects

### Option 1 — From Docker Hub (production / demo)

Use `examples/docker-compose.yml`. It pulls the latest image automatically (`pull_policy: always`).

Create the required directories for each project you want to run:

```
examples/
├── big-trades/
│   ├── configDocker/    ← create this, add config.yaml
│   └── dataDocker/      ← create this (empty is fine)
├── count-trades/
│   ├── configDocker/
│   └── dataDocker/
...
```

Run a single project:

```bat
cd examples
docker compose run --rm big-trades
```

Run all projects at once:

```bat
cd examples
docker compose up
```

### Option 2 — From a locally built image (development)

Each project's example folder contains `run_docker_local.bat` and a `docker-compose.yml` that references the local image tag.

```bat
REM 1. Build
docker build -t big-trades -f build/big-trades .

REM 2. Run
cd examples/big-trades
run_docker_local.bat
```

### Option 3 — Run an arbitrary command inside the container

```bat
docker compose run --rm --entrypoint sh big-trades-local -lc "R -q -e 'installed.packages()'"
```

---

## Adding a new project

1. Create `source/<project-name>/` with at least `main.R` (or `main.py`).
2. Create `build/<project-name>` (Dockerfile, no extension). Use an existing Dockerfile as a template. Build context is always repo root.
3. Add an entry to `.github/workflows/release-dockerhub.yml` under `strategy.matrix.include`.
4. Create `examples/<project-name>/` with:
   - `docker-compose.yml` (local image)
   - `run_docker_local.bat`
   - `run_docker_example.bat`
5. Add a service entry to `examples/docker-compose.yml` (Docker Hub image).
6. Create `source/<project-name>/configDocker/` locally (do not commit), populate with required config files.
