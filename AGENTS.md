# AGENTS.md

Instructions for AI coding agents (Claude Code, Cursor, Copilot, etc.) working with ssh-parallel-test.

## What this tool does

ssh-parallel-test (spt) distributes slow e2e test suites across multiple machines via SSH. It discovers tests, groups them, and schedules them using LPT bin packing. Everything runs inside Docker.

## When to use spt

Use spt when the project has e2e tests that take more than a few minutes to run locally. Distributing across N machines gives up to Nx speedup on wall-clock time. This is especially valuable during iterative development where you need fast feedback on whether your changes break e2e tests.

## Integration pattern

spt is project-agnostic. All project-specific behavior is configured via YAML:

1. **`project.yml`** — lives in the project repo, defines how to discover, run, seed, and clean tests
2. **Cluster configs** — reference `project.yml` and add machine lists, gitignored

### Minimal project.yml

```yaml
remote:
  workdir: ~/my-project

discover:
  command: "pytest --collect-only -q"
  group_regex: "(group1|group2|group3)"

run:
  command: "pytest {tests} -v --durations=0"
  duration_regex: "\\s*([\\d.]+)s call\\s+(.+)"

seed:
  setup: "make build"

clean:
  command: "make clean"
```

### Minimal cluster config

```yaml
project: project.yml
machines:
  - host: 10.0.0.1
    user: root
    slots: 3
```

### Makefile integration

```makefile
SPT_DIR ?= $(HOME)/ssh-parallel-test
SPT_IMAGE := spt:latest
REMOTE_CFG ?= path/to/config.yml
SPT_RUN := docker run --rm -e HOME=$(HOME) \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $(CURDIR):$(CURDIR) -v /tmp:/tmp \
  -v $(HOME)/.ssh:$(HOME)/.ssh:ro \
  -v $(HOME)/.ssh-parallel-test:$(HOME)/.ssh-parallel-test \
  --network host -w $(CURDIR) $(SPT_IMAGE)

_build-spt:
	docker build -t $(SPT_IMAGE) $(SPT_DIR)

remote-run: _build-spt
	$(SPT_RUN) -c $(REMOTE_CFG) run
```

## Key concepts

- **Groups**: Tests are grouped by regex (e.g. by plugin, module, or tag). Each group runs serially on a machine; groups run in parallel across machines.
- **Slots**: Each machine has N slots (default 3) — the max number of groups it can run concurrently.
- **LPT scheduling**: With historical timings, the heaviest tests are assigned first to the least-loaded machine.
- **Timings**: Stored in `~/.ssh-parallel-test/<project-slug>/timings.json`. Collected automatically from test output via `duration_regex`.

## Template variables

These are substituted in `discover.command` and `run.command`:

| Variable | Replaced with |
|---|---|
| `{root}` | Project root directory (cwd) |
| `{tests}` | Space-separated test IDs assigned to this task |
| `{group}` | Group name for this task |

## Commands reference

| Command | Purpose |
|---|---|
| `spt -c config.yml seed` | rsync repo + run setup on all machines |
| `spt -c config.yml run` | Full pipeline: rsync → discover → schedule → execute |
| `spt -c config.yml clean` | Run cleanup command on all machines |
| `spt -c config.yml status` | Check SSH + Docker on all machines |
| `spt -c config.yml bench N` | Run N iterations, report statistics |
| `spt -c config.yml estimate` | Show wall time estimates for 1..N machines |
| `spt -c config.yml fix-ssh PWD` | Copy SSH key to machines via password auth |

## Do not modify

- `spt.py` — the tool itself. It lives in its own repo (`~/ssh-parallel-test/`), not in the project.
- `timings.json` — auto-managed, do not edit manually.

## Common tasks

- **Add a new test group**: Update `discover.group_regex` in `project.yml` to include the new group name.
- **Add machines**: Add entries to the `machines:` list in the cluster config.
- **Debug a failing remote test**: Run `spt -c config.yml run`, failed test output is printed automatically.
- **Rebuild from scratch on remotes**: `spt -c config.yml seed` (rsyncs + runs `seed.setup`).
