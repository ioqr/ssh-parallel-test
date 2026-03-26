#!/usr/bin/env python3
"""
ssh-parallel-test (spt) — Distribute tests across machines via SSH.

Discovers tests, groups them by a configurable regex, and schedules
them across a cluster of machines using LPT (longest-processing-time
first) bin packing with historical timings. Each machine runs multiple
groups concurrently (up to `slots`); tests within a group run serially.

All project-specific behavior (discovery, execution, setup, cleanup)
is configured via YAML — the tool itself is project-agnostic.

Usage:
    spt -c config.yml seed      # rsync + setup machines
    spt -c config.yml run       # rsync + discover + schedule + run
    spt -c config.yml status    # check connectivity and deps
    spt -c config.yml clean     # run cleanup on machines
    spt -c config.yml bench N   # run N iterations, report min/avg/max
    spt -c config.yml estimate  # show wall time vs machine count
    spt -c config.yml fix-ssh PASSWORD  # copy SSH key via password auth
"""

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_TEST_DURATION = 30.0  # seconds, for tests with no history

# ---------------------------------------------------------------------------
# Terminal colors
# ---------------------------------------------------------------------------

if sys.stderr.isatty() and not os.environ.get("NO_COLOR"):
    _RED = "\033[0;31m"
    _GREEN = "\033[0;32m"
    _YELLOW = "\033[0;33m"
    _BOLD = "\033[1m"
    _RESET = "\033[0m"
else:
    _RED = _GREEN = _YELLOW = _BOLD = _RESET = ""


def _log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] {msg}", file=sys.stderr, flush=True)


def _die(msg: str) -> None:
    print(f"{_RED}error:{_RESET} {msg}", file=sys.stderr)
    sys.exit(1)


def _fmt_duration(secs: float) -> str:
    s = int(secs)
    if s < 60:
        return f"{s}s"
    return f"{s // 60}m{s % 60:02d}s"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class Machine:
    host: str
    user: str
    slots: int = 3

    @property
    def ssh_dest(self) -> str:
        return f"{self.user}@{self.host}"


@dataclass
class Config:
    machines: list[Machine]
    workdir: str
    ssh_key: Path | None
    rsync_excludes: list[str]
    discover_command: str
    group_regex: str
    run_command: str
    duration_regex: str | None
    seed_setup: str | None
    seed_auto: bool
    docker_install: str | None
    clean_command: str | None
    timings_file: Path
    root: Path


def _deep_merge(base: dict, overlay: dict) -> dict:
    """Deep merge overlay into base. Overlay values win."""
    result = {}
    for k in set(base) | set(overlay):
        if k in overlay and k in base:
            if isinstance(base[k], dict) and isinstance(overlay[k], dict):
                result[k] = _deep_merge(base[k], overlay[k])
            else:
                result[k] = overlay[k]
        elif k in overlay:
            result[k] = overlay[k]
        else:
            result[k] = base[k]
    return result


def load_config(path: str) -> Config:
    conf_path = Path(path)
    if not conf_path.exists():
        _die(f"Config not found: {conf_path}")

    with open(conf_path) as f:
        raw = yaml.safe_load(f)

    conf_dir = conf_path.parent

    # Merge project config if specified
    project_file = raw.get("project")
    if project_file:
        project_path = conf_dir / project_file
        if not project_path.exists():
            _die(f"Project config not found: {project_path}")
        with open(project_path) as f:
            project_raw = yaml.safe_load(f)
        raw = _deep_merge(project_raw, raw)

    # Parse machines
    machines_raw = raw.get("machines", [])
    if not machines_raw:
        _die("No machines configured")

    machines = []
    seen_hosts = set()
    for entry in machines_raw:
        if not isinstance(entry, dict) or "host" not in entry:
            _die(f"Machine entry missing 'host' field: {entry}")
        host = entry["host"]
        if host in seen_hosts:
            _die(f"Duplicate machine host: {host}")
        seen_hosts.add(host)
        slots = entry.get("slots", 3)
        if slots < 1:
            _die(f"Machine {host}: slots must be >= 1, got {slots}")
        machines.append(Machine(
            host=host,
            user=entry.get("user", "root"),
            slots=slots,
        ))

    # Parse sections
    remote = raw.get("remote", {})
    ssh = raw.get("ssh", {})
    rsync = raw.get("rsync", {})
    discover = raw.get("discover", {})
    run = raw.get("run", {})
    seed = raw.get("seed", {})
    clean = raw.get("clean", {})

    if "command" not in discover:
        _die("Missing discover.command in config")
    if "group_regex" not in discover:
        _die("Missing discover.group_regex in config")
    if "command" not in run:
        _die("Missing run.command in config")

    ssh_key_str = ssh.get("key")
    ssh_key = Path(ssh_key_str).expanduser() if ssh_key_str else None

    timings_file_str = raw.get("timings_file")
    if timings_file_str:
        timings_file = conf_dir / timings_file_str
    else:
        # Derive slug from the resolved (symlink-followed) config file path,
        # so each cluster config gets its own timings even when accessed via
        # a config.yml symlink.
        resolved = conf_path.resolve()
        try:
            rel = str(resolved.relative_to(Path.home()))
        except ValueError:
            rel = str(resolved)
        slug = "-" + re.sub(r'[^a-zA-Z0-9-]', '-', rel)
        timings_file = Path.home() / ".ssh-parallel-test" / slug / "timings.json"

    return Config(
        machines=machines,
        workdir=remote.get("workdir", "~/project"),
        ssh_key=ssh_key,
        rsync_excludes=rsync.get("excludes", [".git/", "__pycache__/"]),
        discover_command=discover["command"],
        group_regex=discover["group_regex"],
        run_command=run["command"],
        duration_regex=run.get("duration_regex"),
        seed_setup=seed.get("setup"),
        seed_auto=bool(seed.get("auto", False)),
        docker_install=seed.get("docker_install"),
        clean_command=clean.get("command"),
        timings_file=timings_file,
        root=Path.cwd(),
    )


# ---------------------------------------------------------------------------
# Per-machine remote locking
# ---------------------------------------------------------------------------

LOCK_STALE_SECS = 7200  # 2 hours - assume dead if older than this
LOCK_POLL_SECS = 10

import uuid as _uuid
import socket as _socket


def _lock_dir(cfg: Config) -> str:
    """Remote lock directory derived from workdir."""
    slug = re.sub(r'[^a-zA-Z0-9]', '-', cfg.workdir.strip("~/"))
    return f"/tmp/.spt-lock-{slug}"


def _lock_info(run_id: str) -> str:
    """Build lock info string, escaped for shell."""
    # Use simple key=value format to avoid JSON quoting issues in shell
    host = _socket.gethostname().replace("'", "")
    ts = int(time.time())
    return f"id={run_id} host={host} ts={ts}"


def _parse_lock_info(raw: str) -> dict:
    """Parse lock info from key=value format."""
    info = {}
    for part in raw.strip().split():
        if "=" in part:
            k, v = part.split("=", 1)
            info[k] = v
    return info


def _try_lock_machine(dest: str, lock_dir: str, run_id: str) -> bool:
    """Try to atomically lock a remote machine. Returns True if acquired."""
    info = _lock_info(run_id)
    r = subprocess.run(
        ["ssh", *_SSH_OPTS, dest,
         f"mkdir -p $(dirname {lock_dir}) && mkdir {lock_dir} 2>/dev/null"
         f" && echo '{info}' > {lock_dir}/info && echo LOCKED"
         f" || echo BUSY"],
        capture_output=True, text=True, timeout=15,
    )
    return "LOCKED" in r.stdout


def _check_lock_stale(dest: str, lock_dir: str) -> bool:
    """Check if an existing lock is stale. Returns True if stale."""
    r = subprocess.run(
        ["ssh", *_SSH_OPTS, dest, f"cat {lock_dir}/info 2>/dev/null"],
        capture_output=True, text=True, timeout=15,
    )
    info = _parse_lock_info(r.stdout)
    try:
        ts = float(info.get("ts", "0"))
        age = time.time() - ts
        return age > LOCK_STALE_SECS
    except (ValueError, TypeError):
        return True  # corrupt lock, treat as stale


def _force_lock_machine(dest: str, lock_dir: str, run_id: str) -> bool:
    """Force-acquire a lock (for stale lock takeover)."""
    info = _lock_info(run_id)
    r = subprocess.run(
        ["ssh", *_SSH_OPTS, dest,
         f"rm -rf {lock_dir} && mkdir {lock_dir} && echo '{info}' > {lock_dir}/info && echo LOCKED"],
        capture_output=True, text=True, timeout=15,
    )
    return "LOCKED" in r.stdout


def _unlock_machine(dest: str, lock_dir: str) -> None:
    """Release the lock on a remote machine."""
    subprocess.run(
        ["ssh", *_SSH_OPTS, dest, f"rm -rf {lock_dir}"],
        capture_output=True, timeout=15,
    )


def _try_lock_machines(
    machines: list[Machine], lock_dir: str, run_id: str,
) -> list[Machine]:
    """Try to lock multiple machines in parallel. Returns those acquired."""
    locked = []

    def _try(m: Machine) -> Machine | None:
        if _try_lock_machine(m.ssh_dest, lock_dir, run_id):
            return m
        # Check for stale lock
        if _check_lock_stale(m.ssh_dest, lock_dir):
            _log(f"Stale lock on {m.host}, taking over")
            if _force_lock_machine(m.ssh_dest, lock_dir, run_id):
                return m
        return None

    with ThreadPoolExecutor(max_workers=max(1, len(machines))) as pool:
        for result in pool.map(_try, machines):
            if result is not None:
                locked.append(result)

    return locked


def _unlock_machines(machines: list[Machine], lock_dir: str) -> None:
    """Release locks on multiple machines in parallel."""
    with ThreadPoolExecutor(max_workers=max(1, len(machines))) as pool:
        pool.map(lambda m: _unlock_machine(m.ssh_dest, lock_dir), machines)


# ---------------------------------------------------------------------------
# Test timings (for intelligent scheduling)
# ---------------------------------------------------------------------------

def _load_timings(cfg: Config) -> dict[str, float]:
    if cfg.timings_file.exists():
        with open(cfg.timings_file) as f:
            return json.load(f)
    return {}


def _save_timings(cfg: Config, timings: dict[str, float]) -> None:
    cfg.timings_file.parent.mkdir(parents=True, exist_ok=True)
    with open(cfg.timings_file, "w") as f:
        json.dump(timings, f, indent=2, sort_keys=True)
    _log(f"Saved timings to {cfg.timings_file}")


def _parse_durations(output: str, duration_regex: str | None) -> dict[str, float]:
    """Parse test durations from output using the configured regex."""
    if not duration_regex:
        return {}
    timings = {}
    pattern = re.compile(duration_regex)
    for line in output.split("\n"):
        m = pattern.match(line)
        if m:
            timings[m.group(2).strip()] = float(m.group(1))
    return timings


# ---------------------------------------------------------------------------
# Test discovery and scheduling
# ---------------------------------------------------------------------------

def discover_tests(cfg: Config) -> dict[str, list[str]]:
    """Run the discover command and group test IDs by the configured regex."""
    cmd = cfg.discover_command.format(root=str(cfg.root))
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if r.returncode != 0:
        _die(f"Test collection failed:\n{r.stdout}\n{r.stderr}")

    pattern = re.compile(cfg.group_regex)
    by_group: dict[str, list[str]] = {}
    for line in r.stdout.strip().split("\n"):
        line = line.strip()
        if "::" not in line:
            continue
        m = pattern.search(line)
        if m:
            group = m.group(1)
            by_group.setdefault(group, []).append(line)

    return {g: tests for g, tests in sorted(by_group.items()) if tests}


@dataclass
class TestAssignment:
    machine: Machine
    group: str
    test_ids: list[str]


def schedule(
    machines: list[Machine],
    tests_by_group: dict[str, list[str]],
    timings: dict[str, float] | None = None,
) -> list[TestAssignment]:
    """Distribute tests across machines using LPT bin packing.

    Each machine runs up to ``slots`` groups concurrently. Tests within each
    group are assigned using longest-processing-time-first scheduling when
    historical timings are available, otherwise round-robin.

    All machines are eligible for all groups. The ``slots`` limit controls
    how many distinct groups a machine can run concurrently, not which
    groups it may receive.
    """
    if timings is None:
        timings = {}
    if not machines or not tests_by_group:
        return []

    machine_by_host = {m.host: m for m in machines}

    # Global LPT: flatten all tests, sort longest-first, assign each to
    # the least-loaded machine that can accept the test's group (either
    # already running that group, or has a free slot).
    all_tests = []
    for group, tests in sorted(tests_by_group.items()):
        for t in tests:
            all_tests.append((group, t))

    all_tests.sort(
        key=lambda gt: timings.get(gt[1], DEFAULT_TEST_DURATION),
        reverse=True,
    )

    machine_load: dict[str, float] = {m.host: 0.0 for m in machines}
    machine_groups: dict[str, set[str]] = {m.host: set() for m in machines}
    assignments: dict[tuple[str, str], TestAssignment] = {}

    for group, test_id in all_tests:
        dur = timings.get(test_id, DEFAULT_TEST_DURATION)

        # Find least-loaded machine that can accept this group:
        # either already has this group, or has a free slot.
        best_host = None
        best_load = float("inf")
        for m in machines:
            can_accept = (
                group in machine_groups[m.host]
                or len(machine_groups[m.host]) < m.slots
            )
            if can_accept and machine_load[m.host] < best_load:
                best_host = m.host
                best_load = machine_load[m.host]

        if best_host is None:
            # All machines at slot capacity for new groups; pick least-loaded
            # machine that already has this group.
            for m in machines:
                if group in machine_groups[m.host] and machine_load[m.host] < best_load:
                    best_host = m.host
                    best_load = machine_load[m.host]

        if best_host is None:
            # Last resort: least-loaded machine overall (exceed slots).
            best_host = min(machine_load, key=machine_load.get)

        machine_load[best_host] += dur
        machine_groups[best_host].add(group)
        key = (best_host, group)
        if key not in assignments:
            assignments[key] = TestAssignment(machine_by_host[best_host], group, [])
        assignments[key].test_ids.append(test_id)

    return sorted(assignments.values(), key=lambda a: (a.machine.host, a.group))


# ---------------------------------------------------------------------------
# SSH / rsync helpers
# ---------------------------------------------------------------------------

_SSH_OPTS: list[str] = []


def _setup_ssh(cfg: Config) -> None:
    global _SSH_OPTS
    opts = [
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=10",
        "-o", "LogLevel=ERROR",
        "-o", "BatchMode=yes",
    ]
    if cfg.ssh_key:
        opts += ["-o", "IdentitiesOnly=yes", "-i", str(cfg.ssh_key)]
    _SSH_OPTS = opts


def ssh_run(
    dest: str, command: str, workdir: str = None, timeout: int = 600,
) -> subprocess.CompletedProcess:
    prefix = f"cd {workdir} && " if workdir else ""
    return subprocess.run(
        ["ssh", *_SSH_OPTS, dest, f"{prefix}{command}"],
        capture_output=True, text=True, timeout=timeout,
    )


def ssh_check(dest: str) -> bool:
    try:
        r = subprocess.run(
            ["ssh", *_SSH_OPTS, "-o", "ConnectTimeout=5", dest, "true"],
            capture_output=True, timeout=10,
        )
        return r.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def rsync_to(cfg: Config, dest: str, workdir: str) -> subprocess.CompletedProcess:
    # Ensure workdir exists (and remove any file that might be in the way)
    subprocess.run(
        ["ssh", *_SSH_OPTS, dest,
         f"test -d {workdir} || (rm -f {workdir}; mkdir -p {workdir})"],
        capture_output=True, timeout=15,
    )
    cmd = ["rsync", "-az", "--delete"]
    for exc in cfg.rsync_excludes:
        cmd += ["--exclude", exc]
    cmd += [f"{cfg.root}/", f"{dest}:{workdir}/"]
    return subprocess.run(
        cmd, capture_output=True, text=True,
        env={**os.environ, "RSYNC_RSH": f"ssh {' '.join(_SSH_OPTS)}"},
    )


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------

@dataclass
class TaskResult:
    host: str
    group: str
    test_count: int
    ok: bool
    duration: float
    output: str = ""


@dataclass
class RunResult:
    rsync_results: list[TaskResult] = field(default_factory=list)
    e2e_results: list[TaskResult] = field(default_factory=list)
    total_duration: float = 0.0
    total_tests: int = 0
    passed_tests: int = 0


def print_summary(result: RunResult) -> None:
    total = result.total_tests
    passed = result.passed_tests
    n_machines = len({r.host for r in result.rsync_results}) if result.rsync_results else 0

    print()
    print(f"  {_BOLD}{'=' * 68}{_RESET}")
    print(f"  {_BOLD}  spt summary ({total} tests across {n_machines} machines){_RESET}")
    print(f"  {'=' * 68}")
    print(f"  {'phase':<8} {'host':<17} {'group':<9} {'tests':>5}  {'result':<8} {'duration':>8}")
    print(f"  {'─' * 8} {'─' * 17} {'─' * 9} {'─' * 5}  {'─' * 8} {'─' * 8}")

    for r in result.rsync_results:
        status = f"{_GREEN}ok{_RESET}" if r.ok else f"{_RED}FAIL{_RESET}"
        print(f"  {'rsync':<8} {r.host:<17} {'':<9} {'':>5}  {status:<17} {_fmt_duration(r.duration):>8}")

    for r in sorted(result.e2e_results, key=lambda r: (r.group, r.host)):
        status = f"{_GREEN}PASS{_RESET}" if r.ok else f"{_RED}FAIL{_RESET}"
        print(
            f"  {'e2e':<8} {r.host:<17} {r.group:<9} {r.test_count:>5}  "
            f"{status:<17} {_fmt_duration(r.duration):>8}"
        )

    print(f"  {'─' * 8} {'─' * 17} {'─' * 9} {'─' * 5}  {'─' * 8} {'─' * 8}")

    if result.rsync_results:
        rsync_wall = max(r.duration for r in result.rsync_results)
        print(f"  {'rsync (wall)':<44} {_fmt_duration(rsync_wall):>18}")
    if result.e2e_results:
        e2e_wall = max(r.duration for r in result.e2e_results)
        print(f"  {'e2e   (wall)':<44} {_fmt_duration(e2e_wall):>18}")
    print(f"  {'total (wall)':<44} {_fmt_duration(result.total_duration):>18}")

    print(f"  {'=' * 68}")
    failed = [r for r in result.e2e_results if not r.ok]
    if failed:
        fail_desc = ", ".join(f"{r.group}@{r.host}" for r in failed)
        print(f"  {_RED}{_BOLD}result: FAIL ({passed}/{total} passed, failed: {fail_desc}){_RESET}")
    else:
        print(f"  {_GREEN}{_BOLD}result: PASS ({passed}/{total} tests){_RESET}")
    print(f"  {'=' * 68}")
    print()


# ---------------------------------------------------------------------------
# Live dashboard
# ---------------------------------------------------------------------------

_INTERACTIVE = sys.stderr.isatty() and not os.environ.get("NO_COLOR")


@dataclass
class _TaskStatus:
    host: str
    group: str
    total_tests: int
    estimated_secs: float
    completed_tests: int = 0
    current_test: str = ""
    start_time: float = 0.0
    end_time: float = 0.0
    done: bool = False
    passed: bool = False


def _bar(done: int, total: int, width: int = 8) -> str:
    if total == 0:
        return "░" * width
    filled = round(width * done / total)
    return "█" * filled + "░" * (width - filled)


def _short_test(test_id: str, maxlen: int = 24) -> str:
    if "::" in test_id:
        test_id = test_id.split("::")[-1]
    return test_id[:maxlen - 1] + "…" if len(test_id) > maxlen else test_id


class Dashboard:
    """Live terminal display for parallel test execution."""

    def __init__(self, statuses: list[_TaskStatus], total_tests: int):
        self.statuses = statuses
        self.total_tests = total_tests
        self.t0 = time.monotonic()
        self._lock = threading.Lock()
        self._drawn = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=2)
        with self._lock:
            if self._drawn:
                sys.stderr.write(f"\033[{self._drawn}A\033[J")
                sys.stderr.flush()
                self._drawn = 0

    def _loop(self):
        while not self._stop.is_set():
            self._render()
            self._stop.wait(0.5)
        self._render()

    def _render(self):
        with self._lock:
            if self._drawn:
                sys.stderr.write(f"\033[{self._drawn}A")

            now = time.monotonic()
            done_count = sum(s.completed_tests for s in self.statuses)
            elapsed = now - self.t0
            eta = self._eta(now)

            lines = []

            # Header
            pct = int(100 * done_count / self.total_tests) if self.total_tests else 0
            hdr = f" {done_count}/{self.total_tests} tests ({pct}%) ── {_fmt_duration(elapsed)}"
            if eta > 0:
                hdr += f" ── ~{_fmt_duration(eta)} left"
            lines.append(f"  ──{hdr} {'─' * max(1, 60 - len(hdr))}──")
            lines.append("")

            # Task lines
            for s in self.statuses:
                te = ((s.end_time if s.done else now) - s.start_time) if s.start_time else 0.0
                bar = _bar(s.completed_tests, s.total_tests)
                cnt = f"{s.completed_tests}/{s.total_tests}"

                if s.done:
                    tag = f"{_GREEN}✓ PASS{_RESET}" if s.passed else f"{_RED}✗ FAIL{_RESET}"
                    lines.append(
                        f"  {s.host:<15} {s.group:<8} {bar} {cnt:<5}"
                        f" {tag}                {_fmt_duration(te):>7}"
                    )
                else:
                    name = _short_test(s.current_test) if s.current_test else "starting…"
                    lines.append(
                        f"  {s.host:<15} {s.group:<8} {bar} {cnt:<5}"
                        f" {name:<24} {_fmt_duration(te):>7}"
                    )

            lines.append("")

            out = "\n".join(f"{l}\033[K" for l in lines) + "\n"
            sys.stderr.write(out)
            sys.stderr.flush()
            self._drawn = len(lines)

    def _eta(self, now: float) -> float:
        worst = 0.0
        for s in self.statuses:
            if s.done:
                continue
            if s.start_time:
                remaining = max(0, s.estimated_secs - (now - s.start_time))
            else:
                remaining = s.estimated_secs
            worst = max(worst, remaining)
        return worst


def _run_assignment_live(
    cfg: Config, a: TestAssignment, status: _TaskStatus,
) -> TaskResult:
    """Stream SSH output line-by-line, updating dashboard status."""
    tests = " ".join(a.test_ids)
    cmd = cfg.run_command.format(tests=tests, group=a.group)
    prefix = f"cd {cfg.workdir} && " if cfg.workdir else ""

    status.start_time = time.monotonic()
    try:
        proc = subprocess.Popen(
            ["ssh", *_SSH_OPTS, a.machine.ssh_dest, f"{prefix}{cmd}"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        buf: list[str] = []
        for line in proc.stdout:
            buf.append(line)
            s = line.strip()
            if " PASSED" in s or " FAILED" in s or " ERROR" in s:
                status.completed_tests += 1
                if "::" in s:
                    status.current_test = s.split()[0]
            elif "::" in s and not s.startswith((" ", "=", "-")):
                status.current_test = s.split()[0] if " " in s else s
        proc.wait()
        ok = proc.returncode == 0
        output = "".join(buf)
    except Exception:
        ok = False
        output = "SSH command failed"

    status.end_time = time.monotonic()
    status.done = True
    status.passed = ok

    dur = status.end_time - status.start_time
    return TaskResult(a.machine.host, a.group, len(a.test_ids), ok, dur, output)


# ---------------------------------------------------------------------------
# Parallel execution
# ---------------------------------------------------------------------------

def _parallel_rsync(cfg: Config) -> list[TaskResult]:
    results: list[TaskResult] = []

    def _do(m: Machine) -> TaskResult:
        _log(f"rsync  -> {m.ssh_dest}")
        t0 = time.monotonic()
        r = rsync_to(cfg, m.ssh_dest, cfg.workdir)
        dur = time.monotonic() - t0
        ok = r.returncode == 0
        _log(f"rsync  -> {m.ssh_dest} {'ok' if ok else 'FAIL'} ({_fmt_duration(dur)})")
        return TaskResult(m.host, "", 0, ok, dur, r.stderr if not ok else "")

    with ThreadPoolExecutor(max_workers=len(cfg.machines)) as pool:
        futures = {pool.submit(_do, m): m for m in cfg.machines}
        for fut in as_completed(futures):
            results.append(fut.result())

    return sorted(results, key=lambda r: r.host)


def _run_assignment(cfg: Config, a: TestAssignment) -> TaskResult:
    """Run a set of tests for one group on one machine via SSH."""
    tests = " ".join(a.test_ids)
    cmd = cfg.run_command.format(tests=tests, group=a.group)

    _log(f"e2e    {a.group} ({len(a.test_ids)} tests) @ {a.machine.ssh_dest}")
    t0 = time.monotonic()
    try:
        r = ssh_run(a.machine.ssh_dest, cmd, workdir=cfg.workdir, timeout=1800)
        ok = r.returncode == 0
        output = r.stdout + r.stderr
    except subprocess.TimeoutExpired:
        ok = False
        output = "SSH command timed out (1800s)"
    dur = time.monotonic() - t0
    status = f"{_GREEN}PASS{_RESET}" if ok else f"{_RED}FAIL{_RESET}"
    _log(f"e2e    {a.group} ({len(a.test_ids)} tests) @ {a.machine.ssh_dest} {status} ({_fmt_duration(dur)})")
    return TaskResult(a.machine.host, a.group, len(a.test_ids), ok, dur, output)


def _parallel_e2e(
    cfg: Config, assignments: list[TestAssignment],
    timings: dict[str, float] | None = None,
) -> list[TaskResult]:
    if not assignments:
        return []

    # Interactive: live dashboard with streaming output
    if _INTERACTIVE:
        if timings is None:
            timings = {}
        statuses = []
        for a in assignments:
            est = sum(timings.get(t, DEFAULT_TEST_DURATION) for t in a.test_ids)
            statuses.append(_TaskStatus(a.machine.host, a.group, len(a.test_ids), est))

        total = sum(len(a.test_ids) for a in assignments)
        dash = Dashboard(statuses, total)
        dash.start()

        results: list[TaskResult] = []
        with ThreadPoolExecutor(max_workers=len(assignments)) as pool:
            futures = {
                pool.submit(_run_assignment_live, cfg, a, statuses[i]): a
                for i, a in enumerate(assignments)
            }
            for fut in as_completed(futures):
                results.append(fut.result())

        dash.stop()

        # Log final results after dashboard clears
        for r in sorted(results, key=lambda r: (r.group, r.host)):
            tag = f"{_GREEN}PASS{_RESET}" if r.ok else f"{_RED}FAIL{_RESET}"
            _log(f"e2e    {r.group} ({r.test_count} tests) @ {r.host} {tag} ({_fmt_duration(r.duration)})")

        return sorted(results, key=lambda r: (r.host, r.group))

    # Non-interactive: batch mode
    results = []
    with ThreadPoolExecutor(max_workers=len(assignments)) as pool:
        futures = {pool.submit(_run_assignment, cfg, a): a for a in assignments}
        for fut in as_completed(futures):
            results.append(fut.result())
    return sorted(results, key=lambda r: (r.host, r.group))


def _parallel_ssh(
    machines: list[Machine], command: str, workdir: str,
    label: str, timeout: int = 600, progress_interval: int = 0,
) -> list[TaskResult]:
    results: list[TaskResult] = []
    start_times: dict[str, float] = {}

    def _do(m: Machine) -> TaskResult:
        _log(f"{label}  {m.ssh_dest}")
        t0 = time.monotonic()
        start_times[m.host] = t0
        try:
            r = ssh_run(m.ssh_dest, command, workdir=workdir, timeout=timeout)
            ok = r.returncode == 0
            output = r.stdout + r.stderr
        except subprocess.TimeoutExpired:
            ok = False
            output = f"SSH command timed out ({timeout}s)"
        dur = time.monotonic() - t0
        _log(f"{label}  {m.ssh_dest} {'ok' if ok else 'FAIL'} ({_fmt_duration(dur)})")
        return TaskResult(m.host, "", 0, ok, dur, output)

    with ThreadPoolExecutor(max_workers=len(machines)) as pool:
        futures = {pool.submit(_do, m): m for m in machines}

        if progress_interval and len(machines) > 1:
            while not all(f.done() for f in futures):
                time.sleep(progress_interval)
                if all(f.done() for f in futures):
                    break
                now = time.monotonic()
                done = sum(1 for f in futures if f.done())
                still = [
                    futures[f] for f in futures if not f.done()
                ]
                longest = max(still, key=lambda m: now - start_times.get(m.host, now))
                longest_dur = now - start_times.get(longest.host, now)
                _log(
                    f"{label}  {done}/{len(futures)} done, "
                    f"{len(still)} running "
                    f"(longest: {longest.host} {_fmt_duration(longest_dur)})"
                )

        for fut in as_completed(futures):
            results.append(fut.result())

    return sorted(results, key=lambda r: r.host)


# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

def _check_ssh(cfg: Config) -> None:
    """Verify SSH to all machines. Warn and remove unreachable ones."""
    _log("Checking SSH connectivity...")
    failed = []
    with ThreadPoolExecutor(max_workers=len(cfg.machines)) as pool:
        futures = {pool.submit(ssh_check, m.ssh_dest): m for m in cfg.machines}
        for fut in as_completed(futures):
            m = futures[fut]
            if not fut.result():
                failed.append(m)

    if failed:
        hosts = ", ".join(m.host for m in failed)
        _log(f"{_YELLOW}warning:{_RESET} SSH unreachable: {hosts} (skipping)")
        for m in failed:
            cfg.machines.remove(m)

    if not cfg.machines:
        _die("No reachable machines remaining")


def _ensure_docker(cfg: Config) -> None:
    """Install Docker on machines that don't have it."""
    if not cfg.docker_install:
        return
    _log("Checking Docker...")

    def _check(m):
        r = ssh_run(m.ssh_dest, "docker --version >/dev/null 2>&1")
        return m, r.returncode == 0

    with ThreadPoolExecutor(max_workers=len(cfg.machines)) as pool:
        results = list(pool.map(_check, cfg.machines))

    need_docker = [m for m, ok in results if not ok]
    if not need_docker:
        return

    hosts = ", ".join(m.host for m in need_docker)
    _log(f"Installing Docker on: {hosts}")
    install_results = _parallel_ssh(
        need_docker, cfg.docker_install, workdir=None, label="docker", timeout=300,
        progress_interval=30,
    )
    for r in install_results:
        if not r.ok:
            print(f"\n--- docker install ({r.host}) ---\n{r.output}---\n", file=sys.stderr)
            _die(f"Docker install failed on {r.host}")

    _log("Docker installed. Verifying...")
    for m in need_docker:
        r = ssh_run(m.ssh_dest, "docker --version")
        if r.returncode != 0:
            _die(f"Docker not working on {m.host} after install: {r.stderr}")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_seed(cfg: Config) -> None:
    t0 = time.monotonic()

    _check_ssh(cfg)
    _ensure_docker(cfg)

    _log("Syncing repo to all machines...")
    rsync_results = _parallel_rsync(cfg)
    if any(not r.ok for r in rsync_results):
        for r in rsync_results:
            if not r.ok:
                print(f"  rsync to {r.host} failed: {r.output}", file=sys.stderr)
        _die("rsync failed")

    if cfg.seed_setup:
        _log("Running setup on all machines...")
        setup_results = _parallel_ssh(
            cfg.machines, cfg.seed_setup,
            cfg.workdir, "setup", timeout=1800, progress_interval=30,
        )
        for sr in setup_results:
            if not sr.ok:
                print(f"\n--- setup output ({sr.host}) ---\n{sr.output}---\n", file=sys.stderr)
                _die(f"Setup failed on {sr.host}")

    total = time.monotonic() - t0
    _log(f"Seed complete in {_fmt_duration(total)}.")


def _prep_machines(cfg: Config, machines: list[Machine]) -> list[Machine]:
    """rsync + auto-seed a set of machines. Returns those that succeeded."""
    # rsync
    orig_machines = cfg.machines
    cfg.machines = machines
    rsync_results = _parallel_rsync(cfg)
    cfg.machines = orig_machines
    ok = [m for m in machines if any(
        r.host == m.host and r.ok for r in rsync_results
    )]
    if not ok:
        return [], rsync_results

    # auto-seed
    if cfg.seed_auto:
        _ensure_docker(cfg)
    if cfg.seed_auto and cfg.seed_setup:
        _log(f"Auto-seeding {len(ok)} machine(s)...")
        setup_results = _parallel_ssh(ok, cfg.seed_setup, cfg.workdir, "seed", timeout=1800, progress_interval=30)
        failed = [sr for sr in setup_results if not sr.ok]
        for sr in failed:
            print(f"\n--- seed output ({sr.host}) ---\n{sr.output}---\n", file=sys.stderr)
            _log(f"{_YELLOW}warning:{_RESET} seed failed on {sr.host}, skipping")
        failed_hosts = {sr.host for sr in failed}
        ok = [m for m in ok if m.host not in failed_hosts]
        if ok:
            _log("Auto-seed complete.")

    return ok, rsync_results


def cmd_run(cfg: Config) -> RunResult:
    t0 = time.monotonic()
    run_id = str(_uuid.uuid4())[:8]
    lock_dir = _lock_dir(cfg)

    _check_ssh(cfg)

    # Discover tests upfront
    _log("Discovering tests...")
    tests_by_group = discover_tests(cfg)
    total_tests = sum(len(t) for t in tests_by_group.values())
    _log(f"Found {total_tests} tests across {len(tests_by_group)} groups")

    timings = _load_timings(cfg)
    if timings:
        _log(f"Loaded timings for {len(timings)} tests (LPT scheduling)")

    # Remaining tests to schedule (mutable copy)
    remaining = {g: list(tests) for g, tests in tests_by_group.items()}

    all_machines = list(cfg.machines)
    locked_hosts: set[str] = set()
    all_rsync_results: list[TaskResult] = []
    all_e2e_results: list[TaskResult] = []
    executor = ThreadPoolExecutor(max_workers=max(1, len(all_machines)))
    running: dict = {}  # future -> TestAssignment

    try:
        while any(remaining.values()) or running:
            # Try to lock free machines
            unlocked = [m for m in all_machines if m.host not in locked_hosts]
            if unlocked and any(remaining.values()):
                newly_locked = _try_lock_machines(unlocked, lock_dir, run_id)
                if newly_locked:
                    hosts = ", ".join(m.host for m in newly_locked)
                    _log(f"Locked {len(newly_locked)} machine(s): {hosts}")
                    locked_hosts.update(m.host for m in newly_locked)

                    # Prep (rsync + seed)
                    prepped, rsync_r = _prep_machines(cfg, newly_locked)
                    all_rsync_results.extend(rsync_r)

                    if prepped:
                        # Schedule remaining tests onto new machines
                        assignments = schedule(prepped, remaining, timings)
                        if assignments:
                            _log(f"Scheduled {len(assignments)} tasks:")
                            for a in assignments:
                                _log(f"  {a.machine.host} / {a.group}: {len(a.test_ids)} tests")
                                # Remove assigned tests from remaining
                                for t in a.test_ids:
                                    if t in remaining.get(a.group, []):
                                        remaining[a.group].remove(t)
                                # Launch
                                fut = executor.submit(_run_assignment, cfg, a)
                                running[fut] = a

                    # Clean up empty groups
                    remaining = {g: t for g, t in remaining.items() if t}

            # Check for completed assignments
            done = [f for f in running if f.done()]
            for f in done:
                a = running.pop(f)
                result = f.result()
                all_e2e_results.append(result)

                # Unlock machine if no more running assignments on it
                host_still_running = any(
                    running[ff].machine.host == a.machine.host for ff in running
                )
                if not host_still_running:
                    _unlock_machine(a.machine.ssh_dest, lock_dir)
                    locked_hosts.discard(a.machine.host)

                tag = f"{_GREEN}PASS{_RESET}" if result.ok else f"{_RED}FAIL{_RESET}"
                _log(f"e2e    {result.group} ({result.test_count} tests) @ {result.host} {tag} ({_fmt_duration(result.duration)})")

            # If we have pending tests but no machines available, wait
            if any(remaining.values()) and not done:
                if not running:
                    _log(f"All machines busy, waiting {LOCK_POLL_SECS}s...")
                time.sleep(LOCK_POLL_SECS)

    finally:
        # Always unlock everything we hold
        to_unlock = [m for m in all_machines if m.host in locked_hosts]
        if to_unlock:
            _unlock_machines(to_unlock, lock_dir)
        executor.shutdown(wait=False)

    # Collect timings from output
    for r in all_e2e_results:
        parsed = _parse_durations(r.output, cfg.duration_regex)
        timings.update(parsed)
    _save_timings(cfg, timings)

    total_dur = time.monotonic() - t0
    passed = sum(r.test_count for r in all_e2e_results if r.ok)

    result = RunResult(
        rsync_results=all_rsync_results,
        e2e_results=all_e2e_results,
        total_duration=total_dur,
        total_tests=total_tests,
        passed_tests=passed,
    )

    # Print full output for failed tasks
    for r in all_e2e_results:
        if not r.ok and r.output:
            print(f"\n{'=' * 60}", file=sys.stderr)
            print(f"  FAILED: {r.group} @ {r.host}", file=sys.stderr)
            print(f"{'=' * 60}", file=sys.stderr)
            print(r.output, file=sys.stderr)

    print_summary(result)
    return result


def cmd_clean(cfg: Config) -> None:
    if not cfg.clean_command:
        _die("No clean.command configured")
    _log("Cleaning on all machines...")
    results = _parallel_ssh(
        cfg.machines, cfg.clean_command, cfg.workdir, "clean", timeout=120,
    )
    for r in results:
        if not r.ok:
            _log(f"Warning: clean on {r.host} failed: {r.output[:200]}")
    _log("Clean complete.")


def cmd_bench(cfg: Config, iterations: int) -> None:
    _log(f"Running {iterations} iterations...")
    all_total: list[float] = []
    pass_count = 0

    for i in range(1, iterations + 1):
        _log(f"\n{'=' * 40} iteration {i}/{iterations} {'=' * 40}")
        result = cmd_run(cfg)
        all_total.append(result.total_duration)
        if all(r.ok for r in result.e2e_results):
            pass_count += 1

    print()
    print(f"  {_BOLD}{'=' * 45}{_RESET}")
    print(f"  {_BOLD}  bench results ({iterations} iterations){_RESET}")
    print(f"  {'=' * 45}")
    if all_total:
        print(f"  {'min':>8}  {'avg':>8}  {'max':>8}")
        print(f"  {'─' * 8}  {'─' * 8}  {'─' * 8}")
        print(
            f"  {_fmt_duration(min(all_total)):>8}  "
            f"{_fmt_duration(sum(all_total) / len(all_total)):>8}  "
            f"{_fmt_duration(max(all_total)):>8}"
        )
    print(f"  {'=' * 45}")
    print(f"  pass rate: {pass_count}/{iterations} ({100 * pass_count // iterations}%)")
    print(f"  {'=' * 45}")
    print()

    if pass_count < iterations:
        sys.exit(1)


def cmd_fix_ssh(cfg: Config, password: str) -> None:
    """Use password auth to copy SSH key and fix permissions on all machines."""
    import shutil
    if not shutil.which("sshpass"):
        _die("sshpass not installed. Install it: sudo apt-get install -y sshpass")

    if not cfg.ssh_key:
        _die("No ssh.key configured — fix-ssh needs a key path to copy")

    pubkey = cfg.ssh_key.with_suffix(".pub")
    if not pubkey.exists():
        _die(
            f"Public key not found: {pubkey}\n"
            f"Generate it: ssh-keygen -t ed25519 -f {cfg.ssh_key} -N ''"
        )

    key_data = pubkey.read_text().strip()

    def _fix(m: Machine) -> bool:
        _log(f"fix-ssh  {m.ssh_dest}")
        cmd = (
            f"mkdir -p ~/.ssh && chmod 700 ~ ~/.ssh && "
            f"echo '{key_data}' >> ~/.ssh/authorized_keys && "
            f"sort -u -o ~/.ssh/authorized_keys ~/.ssh/authorized_keys && "
            f"chmod 600 ~/.ssh/authorized_keys"
        )
        try:
            r = subprocess.run(
                ["sshpass", "-p", password, "ssh",
                 "-o", "StrictHostKeyChecking=no",
                 "-o", "UserKnownHostsFile=/dev/null",
                 "-o", "PubkeyAuthentication=no",
                 m.ssh_dest, cmd],
                capture_output=True, text=True, timeout=30,
            )
            if r.returncode == 0:
                if ssh_check(m.ssh_dest):
                    _log(f"fix-ssh  {m.ssh_dest} {_GREEN}ok{_RESET}")
                    return True
                else:
                    _log(f"fix-ssh  {m.ssh_dest} {_RED}key auth still failing{_RESET}")
                    return False
            else:
                _log(f"fix-ssh  {m.ssh_dest} {_RED}FAIL{_RESET}: {r.stderr.strip()}")
                return False
        except subprocess.TimeoutExpired:
            _log(f"fix-ssh  {m.ssh_dest} {_RED}timeout{_RESET}")
            return False

    with ThreadPoolExecutor(max_workers=len(cfg.machines)) as pool:
        results = list(pool.map(_fix, cfg.machines))

    ok = sum(results)
    total = len(results)
    _log(f"fix-ssh complete: {ok}/{total} machines ready")
    if ok < total:
        sys.exit(1)


def _estimate_wall_time(
    n_machines: int,
    tests_by_group: dict[str, list[str]],
    timings: dict[str, float],
    slots: int = 3,
) -> float:
    """Simulate LPT scheduling with n_machines and return estimated wall time."""
    machines = [Machine(host=str(i), user="sim", slots=slots) for i in range(n_machines)]
    assignments = schedule(machines, tests_by_group, timings)

    if not assignments:
        return 0.0
    return max(
        sum(timings.get(t, DEFAULT_TEST_DURATION) for t in a.test_ids)
        for a in assignments
    )


def cmd_estimate(cfg: Config) -> None:
    """Show estimated wall time for 1..N machines using historical timings."""
    timings = _load_timings(cfg)
    if not timings:
        _die("No timings found. Run 'run' first to collect test durations.")

    _log("Discovering tests...")
    tests_by_group = discover_tests(cfg)

    total_tests = sum(len(t) for t in tests_by_group.values())
    n_groups = len(tests_by_group)
    slots = max(m.slots for m in cfg.machines) if cfg.machines else 3

    # Find the theoretical minimum: longest single test
    longest_test = 0.0
    longest_name = ""
    for tests in tests_by_group.values():
        for t in tests:
            dur = timings.get(t, DEFAULT_TEST_DURATION)
            if dur > longest_test:
                longest_test = dur
                longest_name = t

    print()
    print(f"  {_BOLD}estimated wall time ({total_tests} tests, {n_groups} groups){_RESET}")
    print(f"  {'─' * 50}")
    print(f"  {'machines':>8}  {'est. wall':>10}  {'speedup':>8}  notes")
    print(f"  {'─' * 8}  {'─' * 10}  {'─' * 8}  {'─' * 20}")

    baseline = _estimate_wall_time(1, tests_by_group, timings, slots)
    prev = baseline

    for n in range(1, total_tests + 1):
        est = _estimate_wall_time(n, tests_by_group, timings, slots)
        speedup = baseline / est if est > 0 else 0
        notes = ""
        if n == 1:
            notes = "(baseline)"
        elif est >= prev:
            notes = f"<- min ({longest_name.split('::')[-1]})"
            print(f"  {n:>8}  {_fmt_duration(est):>10}  {speedup:>7.1f}x  {notes}")
            break
        prev = est
        print(f"  {n:>8}  {_fmt_duration(est):>10}  {speedup:>7.1f}x  {notes}")

    print(f"  {'─' * 50}")
    print(f"  floor: {_fmt_duration(longest_test)} ({longest_name.split('::')[-1]})")
    print()


def cmd_status(cfg: Config) -> None:
    def _check(m: Machine):
        reachable = ssh_check(m.ssh_dest)
        if reachable:
            ssh_ok = f"{_GREEN}ok{_RESET}"
            dr = ssh_run(m.ssh_dest, "docker --version 2>/dev/null | head -1")
            docker_ver = (
                dr.stdout.strip().split("version ")[-1].split(",")[0]
                if dr.returncode == 0 else f"{_RED}no{_RESET}"
            )
        else:
            ssh_ok = f"{_RED}no{_RESET}"
            docker_ver = "-"
        return m, ssh_ok, docker_ver

    with ThreadPoolExecutor(max_workers=len(cfg.machines)) as pool:
        results = list(pool.map(_check, cfg.machines))

    # ANSI color codes add invisible characters that break %-style padding.
    # Use fixed-width labels so columns line up regardless of color.
    ansi_pad = len(_GREEN) + len(_RESET) if _GREEN else 0

    print()
    print(f"  {'host':<17} {'user':<15} {'slots':>5}  {'ssh':<6} {'docker':<12}")
    print(f"  {'─' * 17} {'─' * 15} {'─' * 5}  {'─' * 6} {'─' * 12}")
    for m, ssh_ok, docker_ver in results:
        print(
            f"  {m.host:<17} {m.user:<15} {m.slots:>5}  "
            f"{ssh_ok:<{6 + ansi_pad}} {docker_ver}"
        )
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Distribute tests across machines via SSH",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "-c", "--config", required=True,
        help="Config YAML file",
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("seed", help="rsync + setup machines")
    sub.add_parser("run", help="rsync + discover + distribute + run tests")
    sub.add_parser("clean", help="Run cleanup on remotes")

    bench_p = sub.add_parser("bench", help="Run N iterations, report min/avg/max")
    bench_p.add_argument(
        "n", type=int, nargs="?", default=5, help="Number of iterations (default: 5)",
    )

    sub.add_parser("estimate", help="Show estimated wall time for 1..N machines")
    sub.add_parser("status", help="Check machine connectivity + deps")

    fix_p = sub.add_parser("fix-ssh", help="Copy SSH key + fix permissions (needs password)")
    fix_p.add_argument("password", help="SSH password for the machines")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    cfg = load_config(args.config)
    _setup_ssh(cfg)

    match args.command:
        case "seed":
            cmd_seed(cfg)
        case "run":
            result = cmd_run(cfg)
            if any(not r.ok for r in result.e2e_results):
                sys.exit(1)
        case "clean":
            cmd_clean(cfg)
        case "bench":
            cmd_bench(cfg, args.n)
        case "estimate":
            cmd_estimate(cfg)
        case "status":
            cmd_status(cfg)
        case "fix-ssh":
            cmd_fix_ssh(cfg, args.password)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{_RED}interrupted{_RESET}", file=sys.stderr)
        sys.exit(130)
