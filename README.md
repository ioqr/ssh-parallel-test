# ssh-parallel-test

Distribute slow e2e test suites across multiple machines via SSH. Drop your
wall-clock time from minutes to seconds by parallelizing across cheap VMs.

Modern AI coding agents (Claude Code, Cursor, etc.) write code in tight
edit → test → fix loops. When your e2e suite takes 5+ minutes, every iteration
stalls the agent. spt cuts that feedback loop by distributing tests across a
cluster, so the agent gets results fast and keeps moving.

## How it works

1. **Discover** — runs a command locally to collect test IDs
2. **Group** — groups tests by a configurable regex (e.g. by module, plugin, tag)
3. **Schedule** — LPT bin packing distributes groups across machines using historical timings
4. **Execute** — all (machine, group) pairs run concurrently via SSH
5. **Collect** — results, timings, and output are aggregated; failures printed in full

Everything runs inside Docker — the control machine and remote machines only
need Docker installed.

## Quick start

```bash
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $(pwd):$(pwd) -v ~/.ssh:/root/.ssh:ro \
  -v ~/.ssh-parallel-test:/root/.ssh-parallel-test \
  --network host -w $(pwd) \
  ghcr.io/ioqr/ssh-parallel-test:latest -c config.yml run
```

No host dependencies beyond Docker. The image is published to GHCR on every push to main.

## Config format

```yaml
machines:
  - host: 192.168.1.10
    user: ubuntu
    slots: 3            # max concurrent groups per machine
  - host: 192.168.1.11
    user: ubuntu
    slots: 3

remote:
  workdir: ~/my-project

ssh:
  key: ~/.ssh/id_ed25519   # optional

rsync:
  excludes:
    - ".git/"
    - "__pycache__/"

# Test discovery — command that outputs test IDs, one per line.
# {root} is replaced with the project root (cwd).
discover:
  command: "pytest --collect-only -q"
  group_regex: "(group1|group2)"  # first capture group = group name

# Test execution — command run on remote machines.
# {tests} = space-separated test IDs, {group} = group name.
run:
  command: "pytest {tests} -v --durations=0"
  duration_regex: "\\s*([\\d.]+)s call\\s+(.+)"  # optional: parse timings

# Machine setup (run by 'seed' command, or automatically by 'run' with auto: true)
seed:
  auto: true   # run seed automatically during 'run' (install Docker + setup)
  setup: "make build"
  docker_install: "curl -fsSL https://get.docker.com | sudo sh"

# Cleanup (run by 'clean' command)
clean:
  command: "make clean"
```

### Project config sharing

To avoid duplicating project settings across multiple cluster configs, use
the `project:` key:

```yaml
# cluster-a.yml
project: project.yml    # shared project settings
machines:
  - host: 10.0.0.1
    user: root
    slots: 3
```

The tool deep-merges the project config as a base, then overlays the cluster
config. Machines and other cluster-specific settings go in the cluster file;
discover, run, seed, clean go in the shared project file.

## Commands

| Command | What it does |
|---|---|
| `seed` | rsync repo + run setup command on all machines |
| `run` | rsync + discover tests + LPT schedule + run in parallel |
| `clean` | run cleanup command on all machines |
| `bench N` | run N iterations, report min/avg/max wall time |
| `estimate` | show estimated wall time for 1..N machines |
| `status` | check SSH connectivity and Docker on all machines |
| `fix-ssh PASSWORD` | copy SSH key to machines via password auth |

## Timings and scheduling

Historical timings are auto-collected from test output via `duration_regex`
and stored in `~/.ssh-parallel-test/<config-slug>/timings.json` (derived from
the resolved config file path, so each cluster gets its own timings). Override
with `timings_file:` in config. Without timings, tests are distributed round-robin.

With timings, the LPT scheduler assigns the heaviest tests first to the
least-loaded machine, minimizing overall wall time.

## AI agent integration

See [AGENTS.md](AGENTS.md) for instructions on integrating spt into AI agent
workflows, including Makefile patterns and config templates.

## Running tests

```bash
pip install pyyaml pytest
python -m pytest test_spt.py -v
```

## License

MIT — see [LICENSE](LICENSE).
