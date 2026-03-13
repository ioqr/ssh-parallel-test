"""
Tests for spt.py — all SSH and subprocess calls are mocked.
No remote machines are contacted.
"""

import os
import subprocess
from pathlib import Path
from unittest import mock

import pytest
import yaml

import sys
sys.path.insert(0, str(Path(__file__).parent))

import spt


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MINIMAL_PROJECT = {
    "discover": {
        "command": "echo 'test::group1_a\ntest::group1_b\ntest::group2_a'",
        "group_regex": "(group1|group2)",
    },
    "run": {
        "command": "pytest {tests} -v",
    },
}


def _write_config(tmp_path, extra=None):
    cfg = {
        "machines": [
            {"host": "10.0.0.1", "user": "ubuntu", "slots": 3},
            {"host": "10.0.0.2", "user": "ubuntu", "slots": 3},
        ],
        **MINIMAL_PROJECT,
    }
    if extra:
        cfg.update(extra)
    conf = tmp_path / "config.yml"
    conf.write_text(yaml.dump(cfg))
    return conf


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_config(tmp_path):
    conf = _write_config(tmp_path)
    return spt.load_config(str(conf))


@pytest.fixture
def five_machine_config(tmp_path):
    cfg = {
        "machines": [
            {"host": f"10.0.0.{i}", "user": "ubuntu", "slots": 3}
            for i in range(1, 6)
        ],
        **MINIMAL_PROJECT,
    }
    conf = tmp_path / "config.yml"
    conf.write_text(yaml.dump(cfg))
    return spt.load_config(str(conf))


# ---------------------------------------------------------------------------
# _fmt_duration
# ---------------------------------------------------------------------------

class TestFmtDuration:
    def test_seconds_only(self):
        assert spt._fmt_duration(0) == "0s"
        assert spt._fmt_duration(5) == "5s"
        assert spt._fmt_duration(59) == "59s"

    def test_minutes_and_seconds(self):
        assert spt._fmt_duration(60) == "1m00s"
        assert spt._fmt_duration(61) == "1m01s"
        assert spt._fmt_duration(125) == "2m05s"
        assert spt._fmt_duration(600) == "10m00s"

    def test_float_truncated(self):
        assert spt._fmt_duration(59.9) == "59s"
        assert spt._fmt_duration(61.7) == "1m01s"


# ---------------------------------------------------------------------------
# Machine dataclass
# ---------------------------------------------------------------------------

class TestMachine:
    def test_ssh_dest(self):
        m = spt.Machine(host="10.0.0.1", user="ubuntu", slots=3)
        assert m.ssh_dest == "ubuntu@10.0.0.1"

    def test_ssh_dest_root(self):
        m = spt.Machine(host="10.0.0.1", user="root", slots=1)
        assert m.ssh_dest == "root@10.0.0.1"

    def test_default_slots(self):
        m = spt.Machine(host="10.0.0.1", user="ubuntu")
        assert m.slots == 3


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_missing_config_file(self, tmp_path):
        with pytest.raises(SystemExit):
            spt.load_config(str(tmp_path / "nope.yml"))

    def test_no_machines(self, tmp_path):
        conf = tmp_path / "config.yml"
        conf.write_text(yaml.dump({"machines": [], **MINIMAL_PROJECT}))
        with pytest.raises(SystemExit):
            spt.load_config(str(conf))

    def test_machine_missing_host(self, tmp_path):
        conf = tmp_path / "config.yml"
        conf.write_text(yaml.dump({
            "machines": [{"user": "ubuntu"}],
            **MINIMAL_PROJECT,
        }))
        with pytest.raises(SystemExit):
            spt.load_config(str(conf))

    def test_duplicate_host(self, tmp_path):
        conf = tmp_path / "config.yml"
        conf.write_text(yaml.dump({
            "machines": [
                {"host": "10.0.0.1", "user": "ubuntu"},
                {"host": "10.0.0.1", "user": "ubuntu"},
            ],
            **MINIMAL_PROJECT,
        }))
        with pytest.raises(SystemExit):
            spt.load_config(str(conf))

    def test_invalid_slots(self, tmp_path):
        conf = tmp_path / "config.yml"
        conf.write_text(yaml.dump({
            "machines": [{"host": "10.0.0.1", "slots": 0}],
            **MINIMAL_PROJECT,
        }))
        with pytest.raises(SystemExit):
            spt.load_config(str(conf))

    def test_missing_discover_command(self, tmp_path):
        conf = tmp_path / "config.yml"
        conf.write_text(yaml.dump({
            "machines": [{"host": "10.0.0.1"}],
            "discover": {"group_regex": "(a|b)"},
            "run": {"command": "test"},
        }))
        with pytest.raises(SystemExit):
            spt.load_config(str(conf))

    def test_missing_group_regex(self, tmp_path):
        conf = tmp_path / "config.yml"
        conf.write_text(yaml.dump({
            "machines": [{"host": "10.0.0.1"}],
            "discover": {"command": "echo test"},
            "run": {"command": "test"},
        }))
        with pytest.raises(SystemExit):
            spt.load_config(str(conf))

    def test_missing_run_command(self, tmp_path):
        conf = tmp_path / "config.yml"
        conf.write_text(yaml.dump({
            "machines": [{"host": "10.0.0.1"}],
            "discover": {"command": "echo test", "group_regex": "(a)"},
        }))
        with pytest.raises(SystemExit):
            spt.load_config(str(conf))

    def test_valid_config(self, tmp_path):
        conf = _write_config(tmp_path, {
            "remote": {"workdir": "/opt/test"},
            "ssh": {"key": "~/.ssh/id_test"},
            "rsync": {"excludes": [".git/", "node_modules/"]},
        })
        cfg = spt.load_config(str(conf))
        assert len(cfg.machines) == 2
        assert cfg.machines[0].host == "10.0.0.1"
        assert cfg.workdir == "/opt/test"
        assert cfg.ssh_key == Path.home() / ".ssh" / "id_test"
        assert cfg.rsync_excludes == [".git/", "node_modules/"]

    def test_defaults(self, tmp_path):
        conf = tmp_path / "config.yml"
        conf.write_text(yaml.dump({
            "machines": [{"host": "10.0.0.1"}],
            **MINIMAL_PROJECT,
        }))
        cfg = spt.load_config(str(conf))
        assert cfg.machines[0].user == "root"
        assert cfg.machines[0].slots == 3
        assert cfg.workdir == "~/project"
        assert cfg.ssh_key is None

    def test_project_merge(self, tmp_path):
        # Write project config
        project = tmp_path / "project.yml"
        project.write_text(yaml.dump({
            "remote": {"workdir": "/opt/proj"},
            **MINIMAL_PROJECT,
            "seed": {"setup": "make build"},
        }))
        # Write cluster config that references project
        cluster = tmp_path / "cluster.yml"
        cluster.write_text(yaml.dump({
            "project": "project.yml",
            "machines": [{"host": "10.0.0.1"}],
        }))
        cfg = spt.load_config(str(cluster))
        assert cfg.workdir == "/opt/proj"
        assert cfg.machines[0].host == "10.0.0.1"
        assert cfg.seed_setup == "make build"

    def test_project_merge_overlay(self, tmp_path):
        # Project sets workdir, cluster overrides it
        project = tmp_path / "project.yml"
        project.write_text(yaml.dump({
            "remote": {"workdir": "/opt/proj"},
            **MINIMAL_PROJECT,
        }))
        cluster = tmp_path / "cluster.yml"
        cluster.write_text(yaml.dump({
            "project": "project.yml",
            "machines": [{"host": "10.0.0.1"}],
            "remote": {"workdir": "/opt/override"},
        }))
        cfg = spt.load_config(str(cluster))
        assert cfg.workdir == "/opt/override"

    def test_timings_file_default(self, tmp_path):
        conf = _write_config(tmp_path)
        cfg = spt.load_config(str(conf))
        assert cfg.timings_file == tmp_path / "timings.json"

    def test_timings_file_custom(self, tmp_path):
        conf = _write_config(tmp_path, {"timings_file": "custom-timings.json"})
        cfg = spt.load_config(str(conf))
        assert cfg.timings_file == tmp_path / "custom-timings.json"


# ---------------------------------------------------------------------------
# Deep merge
# ---------------------------------------------------------------------------

class TestDeepMerge:
    def test_simple_overlay(self):
        base = {"a": 1, "b": 2}
        overlay = {"b": 3, "c": 4}
        assert spt._deep_merge(base, overlay) == {"a": 1, "b": 3, "c": 4}

    def test_nested_merge(self):
        base = {"x": {"a": 1, "b": 2}}
        overlay = {"x": {"b": 3, "c": 4}}
        assert spt._deep_merge(base, overlay) == {"x": {"a": 1, "b": 3, "c": 4}}

    def test_overlay_wins_type_mismatch(self):
        base = {"x": {"a": 1}}
        overlay = {"x": "string"}
        assert spt._deep_merge(base, overlay) == {"x": "string"}


# ---------------------------------------------------------------------------
# Test discovery
# ---------------------------------------------------------------------------

SAMPLE_COLLECT_OUTPUT = """\
e2e/test_delivery.py::test_alpha_delivery
e2e/test_delivery.py::test_alpha_delivery_big
e2e/test_delivery.py::test_beta_delivery
e2e/test_delivery.py::test_beta_delivery_big
e2e/test_recording.py::test_alpha_recording
e2e/test_recording.py::test_beta_recording

6 tests collected in 0.06s
"""


class TestDiscoverTests:
    @mock.patch("spt.subprocess.run")
    def test_groups_by_regex(self, mock_run, sample_config):
        mock_run.return_value = subprocess.CompletedProcess(
            [], 0, SAMPLE_COLLECT_OUTPUT, "",
        )
        # Override group_regex for this test
        sample_config.group_regex = "(alpha|beta)"
        result = spt.discover_tests(sample_config)
        assert len(result["alpha"]) == 3
        assert len(result["beta"]) == 3

    @mock.patch("spt.subprocess.run")
    def test_uses_shell(self, mock_run, sample_config):
        mock_run.return_value = subprocess.CompletedProcess(
            [], 0, SAMPLE_COLLECT_OUTPUT, "",
        )
        sample_config.group_regex = "(alpha|beta)"
        spt.discover_tests(sample_config)
        assert mock_run.call_args[1].get("shell") is True

    @mock.patch("spt.subprocess.run")
    def test_collection_failure(self, mock_run, sample_config):
        mock_run.return_value = subprocess.CompletedProcess([], 1, "", "error")
        with pytest.raises(SystemExit):
            spt.discover_tests(sample_config)


# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------

SAMPLE_TESTS = {
    "alpha": [f"e2e/test::alpha_{i}" for i in range(6)],
    "beta": [f"e2e/test::beta_{i}" for i in range(6)],
    "gamma": [f"e2e/test::gamma_{i}" for i in range(7)],
}


class TestSchedule:
    def test_round_robin_two_machines(self):
        machines = [
            spt.Machine(host="10.0.0.1", user="u", slots=3),
            spt.Machine(host="10.0.0.2", user="u", slots=3),
        ]
        assignments = spt.schedule(machines, SAMPLE_TESTS)
        assert len(assignments) == 6

        alpha_a = [a for a in assignments if a.group == "alpha"]
        assert len(alpha_a) == 2
        assert len(alpha_a[0].test_ids) == 3
        assert len(alpha_a[1].test_ids) == 3

        gamma_a = [a for a in assignments if a.group == "gamma"]
        counts = sorted([len(a.test_ids) for a in gamma_a])
        assert counts == [3, 4]

    def test_five_machines(self):
        machines = [
            spt.Machine(host=f"10.0.0.{i}", user="u", slots=3)
            for i in range(1, 6)
        ]
        assignments = spt.schedule(machines, SAMPLE_TESTS)
        assert len(assignments) == 15
        total = sum(len(a.test_ids) for a in assignments)
        assert total == 19

        alpha_a = [a for a in assignments if a.group == "alpha"]
        alpha_total = sum(len(a.test_ids) for a in alpha_a)
        assert alpha_total == 6
        assert all(len(a.test_ids) <= 2 for a in alpha_a)

    def test_slots_limit(self):
        machines = [
            spt.Machine(host="10.0.0.1", user="u", slots=1),
        ]
        assignments = spt.schedule(machines, SAMPLE_TESTS)
        groups = {a.group for a in assignments}
        assert groups == {"alpha"}

    def test_slots_two(self):
        machines = [
            spt.Machine(host="10.0.0.1", user="u", slots=2),
        ]
        assignments = spt.schedule(machines, SAMPLE_TESTS)
        groups = {a.group for a in assignments}
        assert groups == {"alpha", "beta"}

    def test_empty_tests(self):
        machines = [spt.Machine(host="10.0.0.1", user="u", slots=3)]
        assignments = spt.schedule(machines, {})
        assert assignments == []

    def test_no_machines(self):
        assignments = spt.schedule([], SAMPLE_TESTS)
        assert assignments == []

    def test_all_test_ids_preserved(self):
        machines = [
            spt.Machine(host=f"10.0.0.{i}", user="u", slots=3)
            for i in range(1, 4)
        ]
        assignments = spt.schedule(machines, SAMPLE_TESTS)
        all_ids = []
        for a in assignments:
            all_ids.extend(a.test_ids)
        for group, tests in SAMPLE_TESTS.items():
            for t in tests:
                assert t in all_ids, f"Missing: {t}"
        assert len(all_ids) == sum(len(t) for t in SAMPLE_TESTS.values())


# ---------------------------------------------------------------------------
# SSH / rsync command construction
# ---------------------------------------------------------------------------

class TestSSHHelpers:
    def setup_method(self):
        # Initialize SSH opts for tests
        spt._SSH_OPTS = [
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ConnectTimeout=10",
            "-o", "LogLevel=ERROR",
        ]

    @mock.patch("spt.subprocess.run")
    def test_ssh_run_with_workdir(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess([], 0, "ok", "")
        spt.ssh_run("ubuntu@10.0.0.1", "make test", workdir="/root/project")
        cmd = mock_run.call_args[0][0]
        assert "ssh" in cmd[0]
        assert "ubuntu@10.0.0.1" in cmd
        assert "cd /root/project && make test" in " ".join(cmd)

    @mock.patch("spt.subprocess.run")
    def test_ssh_run_no_workdir(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess([], 0, "", "")
        spt.ssh_run("ubuntu@10.0.0.1", "whoami")
        cmd = mock_run.call_args[0][0]
        full = " ".join(cmd)
        assert "cd" not in full.split("whoami")[0]

    @mock.patch("spt.subprocess.run")
    def test_ssh_check_success(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess([], 0)
        assert spt.ssh_check("ubuntu@10.0.0.1") is True

    @mock.patch("spt.subprocess.run")
    def test_ssh_check_failure(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess([], 255)
        assert spt.ssh_check("ubuntu@10.0.0.1") is False

    @mock.patch("spt.subprocess.run")
    def test_ssh_check_timeout(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired("ssh", 10)
        assert spt.ssh_check("ubuntu@10.0.0.1") is False

    @mock.patch("spt.subprocess.run")
    def test_rsync_command(self, mock_run, sample_config):
        mock_run.return_value = subprocess.CompletedProcess([], 0, "", "")
        spt.rsync_to(sample_config, "ubuntu@10.0.0.1", "/root/project")
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "rsync"
        assert "-az" in cmd
        assert "--delete" in cmd
        assert any("ubuntu@10.0.0.1:/root/project/" in arg for arg in cmd)

    @mock.patch("spt.subprocess.run")
    def test_rsync_uses_ssh_opts(self, mock_run, sample_config):
        mock_run.return_value = subprocess.CompletedProcess([], 0, "", "")
        spt.rsync_to(sample_config, "ubuntu@10.0.0.1", "/root/project")
        env = mock_run.call_args[1]["env"]
        assert "RSYNC_RSH" in env
        assert "StrictHostKeyChecking=no" in env["RSYNC_RSH"]


# ---------------------------------------------------------------------------
# SSH setup
# ---------------------------------------------------------------------------

class TestSetupSSH:
    def test_with_key(self, tmp_path):
        cfg = spt.Config(
            machines=[], workdir="", ssh_key=tmp_path / "id_test",
            rsync_excludes=[], discover_command="", group_regex="",
            run_command="", duration_regex=None, seed_setup=None,
            docker_install=None, clean_command=None,
            timings_file=tmp_path / "t.json", root=tmp_path,
        )
        spt._setup_ssh(cfg)
        assert "-i" in spt._SSH_OPTS
        assert str(tmp_path / "id_test") in spt._SSH_OPTS
        assert "IdentitiesOnly=yes" in " ".join(spt._SSH_OPTS)

    def test_without_key(self, tmp_path):
        cfg = spt.Config(
            machines=[], workdir="", ssh_key=None,
            rsync_excludes=[], discover_command="", group_regex="",
            run_command="", duration_regex=None, seed_setup=None,
            docker_install=None, clean_command=None,
            timings_file=tmp_path / "t.json", root=tmp_path,
        )
        spt._setup_ssh(cfg)
        assert "-i" not in spt._SSH_OPTS
        assert "IdentitiesOnly" not in " ".join(spt._SSH_OPTS)


# ---------------------------------------------------------------------------
# Result tracking / summary
# ---------------------------------------------------------------------------

class TestResults:
    def test_task_result_defaults(self):
        r = spt.TaskResult("10.0.0.1", "alpha", 3, True, 120.5)
        assert r.output == ""
        assert r.ok is True

    def test_run_result_defaults(self):
        r = spt.RunResult()
        assert r.rsync_results == []
        assert r.e2e_results == []
        assert r.total_duration == 0.0
        assert r.total_tests == 0
        assert r.passed_tests == 0

    def test_print_summary_pass(self, capsys):
        result = spt.RunResult(
            rsync_results=[spt.TaskResult("10.0.0.1", "", 0, True, 2.0)],
            e2e_results=[spt.TaskResult("10.0.0.1", "alpha", 3, True, 180.0)],
            total_duration=182.0,
            total_tests=3,
            passed_tests=3,
        )
        spt.print_summary(result)
        out = capsys.readouterr().out
        assert "PASS" in out
        assert "3/3" in out

    def test_print_summary_fail(self, capsys):
        result = spt.RunResult(
            rsync_results=[spt.TaskResult("10.0.0.1", "", 0, True, 2.0)],
            e2e_results=[spt.TaskResult("10.0.0.1", "beta", 3, False, 300.0)],
            total_duration=302.0,
            total_tests=3,
            passed_tests=0,
        )
        spt.print_summary(result)
        out = capsys.readouterr().out
        assert "FAIL" in out
        assert "0/3" in out


# ---------------------------------------------------------------------------
# Parallel execution (mocked subprocess)
# ---------------------------------------------------------------------------

class TestParallelRsync:
    @mock.patch("spt.rsync_to")
    def test_all_ok(self, mock_rsync, sample_config):
        mock_rsync.return_value = subprocess.CompletedProcess([], 0, "", "")
        results = spt._parallel_rsync(sample_config)
        assert len(results) == 2
        assert all(r.ok for r in results)

    @mock.patch("spt.rsync_to")
    def test_one_fails(self, mock_rsync, sample_config):
        def side_effect(cfg, dest, workdir):
            if "10.0.0.2" in dest:
                return subprocess.CompletedProcess([], 1, "", "rsync error")
            return subprocess.CompletedProcess([], 0, "", "")

        mock_rsync.side_effect = side_effect
        results = spt._parallel_rsync(sample_config)
        ok_results = [r for r in results if r.ok]
        fail_results = [r for r in results if not r.ok]
        assert len(ok_results) == 1
        assert len(fail_results) == 1


class TestParallelE2E:
    @mock.patch("spt.ssh_run")
    def test_all_pass(self, mock_ssh, sample_config):
        mock_ssh.return_value = subprocess.CompletedProcess([], 0, "passed", "")
        m = spt.Machine(host="10.0.0.1", user="u", slots=3)
        assignments = [
            spt.TestAssignment(m, "alpha", ["test1", "test2"]),
            spt.TestAssignment(m, "beta", ["test3"]),
        ]
        results = spt._parallel_e2e(sample_config, assignments)
        assert len(results) == 2
        assert all(r.ok for r in results)

    @mock.patch("spt.ssh_run")
    def test_one_fails(self, mock_ssh, sample_config):
        def side_effect(dest, command, workdir=None, timeout=600):
            if "test2" in command:
                return subprocess.CompletedProcess([], 1, "test failed", "error")
            return subprocess.CompletedProcess([], 0, "passed", "")

        mock_ssh.side_effect = side_effect
        m = spt.Machine(host="10.0.0.1", user="u", slots=3)
        assignments = [
            spt.TestAssignment(m, "alpha", ["test1"]),
            spt.TestAssignment(m, "beta", ["test2"]),
        ]
        results = spt._parallel_e2e(sample_config, assignments)
        alpha_r = next(r for r in results if r.group == "alpha")
        beta_r = next(r for r in results if r.group == "beta")
        assert alpha_r.ok is True
        assert beta_r.ok is False

    @mock.patch("spt.ssh_run")
    def test_timeout(self, mock_ssh, sample_config):
        mock_ssh.side_effect = subprocess.TimeoutExpired("ssh", 1800)
        m = spt.Machine(host="10.0.0.1", user="u", slots=3)
        assignments = [spt.TestAssignment(m, "alpha", ["test1"])]
        results = spt._parallel_e2e(sample_config, assignments)
        assert len(results) == 1
        assert results[0].ok is False
        assert "timed out" in results[0].output


# ---------------------------------------------------------------------------
# cmd_run (integration with mocks)
# ---------------------------------------------------------------------------

class TestCmdRun:
    @mock.patch("spt._check_ssh")
    @mock.patch("spt._parallel_e2e")
    @mock.patch("spt.discover_tests")
    @mock.patch("spt._parallel_rsync")
    def test_pass(self, mock_rsync, mock_discover, mock_e2e, mock_ssh, sample_config):
        mock_rsync.return_value = [
            spt.TaskResult("10.0.0.1", "", 0, True, 2.0),
            spt.TaskResult("10.0.0.2", "", 0, True, 2.0),
        ]
        mock_discover.return_value = {"alpha": ["t1", "t2"], "beta": ["t3"]}
        mock_e2e.return_value = [
            spt.TaskResult("10.0.0.1", "alpha", 1, True, 30.0),
            spt.TaskResult("10.0.0.2", "alpha", 1, True, 28.0),
            spt.TaskResult("10.0.0.1", "beta", 1, True, 45.0),
        ]

        result = spt.cmd_run(sample_config)
        assert result.total_tests == 3
        assert result.passed_tests == 3
        assert all(r.ok for r in result.e2e_results)

    @mock.patch("spt._check_ssh")
    @mock.patch("spt._parallel_e2e")
    @mock.patch("spt.discover_tests")
    @mock.patch("spt._parallel_rsync")
    def test_rsync_fail_reduces_machines(self, mock_rsync, mock_discover, mock_e2e, mock_ssh, sample_config):
        mock_rsync.return_value = [
            spt.TaskResult("10.0.0.1", "", 0, True, 2.0),
            spt.TaskResult("10.0.0.2", "", 0, False, 1.0, "connection refused"),
        ]
        mock_discover.return_value = {"alpha": ["t1", "t2"]}
        mock_e2e.return_value = [
            spt.TaskResult("10.0.0.1", "alpha", 2, True, 60.0),
        ]

        result = spt.cmd_run(sample_config)
        mock_e2e.assert_called_once()
        assignments = mock_e2e.call_args[0][1]
        hosts = {a.machine.host for a in assignments}
        assert "10.0.0.2" not in hosts


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

class TestCLI:
    def test_no_command_exits(self):
        with mock.patch("sys.argv", ["spt.py", "-c", "config.yml"]):
            with pytest.raises(SystemExit):
                spt.main()

    def test_config_required(self):
        with mock.patch("sys.argv", ["spt.py", "run"]):
            with pytest.raises(SystemExit):
                spt.main()

    def test_bench_default_n(self):
        parser = spt.argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        bench_p = sub.add_parser("bench")
        bench_p.add_argument("n", type=int, nargs="?", default=5)
        args = parser.parse_args(["bench"])
        assert args.n == 5

    def test_bench_custom_n(self):
        parser = spt.argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        bench_p = sub.add_parser("bench")
        bench_p.add_argument("n", type=int, nargs="?", default=5)
        args = parser.parse_args(["bench", "3"])
        assert args.n == 3


# ---------------------------------------------------------------------------
# Timings and estimation
# ---------------------------------------------------------------------------

SAMPLE_TIMINGS = {
    "e2e/test::alpha_0": 70.0,
    "e2e/test::alpha_1": 40.0,
    "e2e/test::alpha_2": 10.0,
    "e2e/test::alpha_3": 10.0,
    "e2e/test::alpha_4": 10.0,
    "e2e/test::alpha_5": 10.0,
    "e2e/test::beta_0": 70.0,
    "e2e/test::beta_1": 40.0,
    "e2e/test::beta_2": 10.0,
    "e2e/test::beta_3": 10.0,
    "e2e/test::beta_4": 10.0,
    "e2e/test::beta_5": 10.0,
    "e2e/test::gamma_0": 75.0,
    "e2e/test::gamma_1": 42.0,
    "e2e/test::gamma_2": 14.0,
    "e2e/test::gamma_3": 12.0,
    "e2e/test::gamma_4": 12.0,
    "e2e/test::gamma_5": 12.0,
    "e2e/test::gamma_6": 6.0,
}


class TestScheduleWithTimings:
    def test_lpt_separates_heavy_tests(self):
        machines = [
            spt.Machine(host="10.0.0.1", user="u", slots=3),
            spt.Machine(host="10.0.0.2", user="u", slots=3),
        ]
        assignments = spt.schedule(machines, SAMPLE_TESTS, SAMPLE_TIMINGS)
        for group in ["alpha", "beta"]:
            group_a = [a for a in assignments if a.group == group]
            assert len(group_a) == 2
            heavy = f"e2e/test::{group}_0"
            medium = f"e2e/test::{group}_1"
            hosts_heavy = [a.machine.host for a in group_a if heavy in a.test_ids]
            hosts_medium = [a.machine.host for a in group_a if medium in a.test_ids]
            assert hosts_heavy != hosts_medium, f"{group}: heavy and medium on same machine"

    def test_preserves_all_tests_with_timings(self):
        machines = [
            spt.Machine(host=f"10.0.0.{i}", user="u", slots=3)
            for i in range(1, 4)
        ]
        assignments = spt.schedule(machines, SAMPLE_TESTS, SAMPLE_TIMINGS)
        all_ids = [t for a in assignments for t in a.test_ids]
        assert len(all_ids) == sum(len(t) for t in SAMPLE_TESTS.values())


class TestEstimateWallTime:
    def test_one_machine(self):
        est = spt._estimate_wall_time(1, SAMPLE_TESTS, SAMPLE_TIMINGS)
        # Gamma: 75 + 42 + 14 + 12 + 12 + 12 + 6 = 173
        assert est == pytest.approx(173.0)

    def test_more_machines_not_worse(self):
        prev = spt._estimate_wall_time(1, SAMPLE_TESTS, SAMPLE_TIMINGS)
        for n in range(2, 8):
            est = spt._estimate_wall_time(n, SAMPLE_TESTS, SAMPLE_TIMINGS)
            assert est <= prev + 0.01, f"n={n} worse than n={n-1}"
            prev = est

    def test_floor_is_longest_test(self):
        longest = max(SAMPLE_TIMINGS.values())  # 75.0
        for n in range(1, 20):
            est = spt._estimate_wall_time(n, SAMPLE_TESTS, SAMPLE_TIMINGS)
            assert est >= longest - 0.01


class TestParseDurations:
    def test_parses_call_durations(self):
        output = """\
=========================== slowest durations ===========================
56.23s call     e2e/test_recording.py::test_alpha_recording
38.45s call     e2e/test_screenshot.py::test_alpha_screenshot
0.01s setup    e2e/test_recording.py::test_alpha_recording
0.00s teardown e2e/test_recording.py::test_alpha_recording
"""
        regex = r"\s*([\d.]+)s call\s+(.+)"
        result = spt._parse_durations(output, regex)
        assert result == {
            "e2e/test_recording.py::test_alpha_recording": 56.23,
            "e2e/test_screenshot.py::test_alpha_screenshot": 38.45,
        }

    def test_empty_output(self):
        assert spt._parse_durations("", r"\s*([\d.]+)s call\s+(.+)") == {}

    def test_no_regex(self):
        assert spt._parse_durations("56.23s call test", None) == {}


# ---------------------------------------------------------------------------
# Timings load/save
# ---------------------------------------------------------------------------

class TestTimings:
    def test_load_missing(self, tmp_path):
        cfg = spt.Config(
            machines=[], workdir="", ssh_key=None,
            rsync_excludes=[], discover_command="", group_regex="",
            run_command="", duration_regex=None, seed_setup=None,
            docker_install=None, clean_command=None,
            timings_file=tmp_path / "timings.json", root=tmp_path,
        )
        assert spt._load_timings(cfg) == {}

    def test_save_and_load(self, tmp_path):
        cfg = spt.Config(
            machines=[], workdir="", ssh_key=None,
            rsync_excludes=[], discover_command="", group_regex="",
            run_command="", duration_regex=None, seed_setup=None,
            docker_install=None, clean_command=None,
            timings_file=tmp_path / "timings.json", root=tmp_path,
        )
        spt._save_timings(cfg, {"test1": 10.0, "test2": 20.0})
        loaded = spt._load_timings(cfg)
        assert loaded == {"test1": 10.0, "test2": 20.0}
