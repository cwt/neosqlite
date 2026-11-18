"""Tests for the Node.js executable gate used by JS operators."""

import subprocess

import pytest

from neosqlite.js_runtime import (
    find_node_runtime,
    is_supported_node_version,
    node_unavailable_reason,
    parse_node_version,
    probe_node,
    reset_node_runtime_cache,
)


@pytest.fixture(autouse=True)
def _clean_runtime_cache():
    """Keep mocked probe results from leaking into other test modules."""
    reset_node_runtime_cache()
    yield
    reset_node_runtime_cache()


class TestParseNodeVersion:
    @pytest.mark.parametrize(
        ("output", "expected"),
        [
            ("v24.22.3\n", (24, 22, 3)),
            ("24.22.3", (24, 22, 3)),
            ("v22.23.5", (22, 23, 5)),
            ("v26.13.1", (26, 13, 1)),
            ("v24.18.0", (24, 18, 0)),
            ("v24.3", (24, 3, 0)),
            ("v24.22.3-beta.1", (24, 22, 3)),
            ("garbage", None),
            ("", None),
            ("v24", None),
        ],
    )
    def test_parse(self, output, expected):
        assert parse_node_version(output) == expected


class TestSupportedVersions:
    @pytest.mark.parametrize(
        "version",
        [(22, 23, 5), (22, 24, 0), (24, 22, 3), (24, 99, 0), (26, 13, 1)],
    )
    def test_supported(self, version):
        assert is_supported_node_version(version)

    @pytest.mark.parametrize(
        "version",
        [
            (22, 23, 4),  # unpatched 22
            (24, 22, 2),  # unpatched 24
            (26, 13, 0),  # unpatched 26
            (18, 20, 0),  # EOL even
            (20, 19, 0),  # EOL even
            (21, 7, 0),  # EOL odd
            (23, 11, 0),  # EOL odd
            (25, 9, 0),  # EOL odd
            (27, 0, 0),  # unknown line
        ],
    )
    def test_rejected(self, version):
        assert not is_supported_node_version(version)


class TestProbe:
    def test_probe_uses_version_flag(self, monkeypatch):
        calls = []

        def fake_run(args, **kwargs):
            calls.append(args)
            return subprocess.CompletedProcess(
                args, 0, stdout="v24.22.3\n", stderr=""
            )

        monkeypatch.setattr(subprocess, "run", fake_run)
        result = probe_node("/usr/bin/node")
        assert result is not None
        assert result.path == "/usr/bin/node"
        assert result.version == (24, 22, 3)
        assert result.version_string == "24.22.3"
        assert calls == [["/usr/bin/node", "--version"]]

    def test_probe_rejects_outdated(self, monkeypatch):
        def fake_run(args, **kwargs):
            return subprocess.CompletedProcess(
                args, 0, stdout="v24.18.0\n", stderr=""
            )

        monkeypatch.setattr(subprocess, "run", fake_run)
        assert probe_node("/usr/bin/node") is None

    def test_probe_handles_missing_executable(self, monkeypatch):
        def fake_run(args, **kwargs):
            raise FileNotFoundError(args[0])

        monkeypatch.setattr(subprocess, "run", fake_run)
        assert probe_node("/nonexistent/node") is None


class TestDiscovery:
    def test_missing_node_reason(self, monkeypatch):
        monkeypatch.setattr(
            "neosqlite.js_runtime.shutil.which", lambda name: None
        )
        reset_node_runtime_cache()
        assert find_node_runtime() is None
        assert "not found" in node_unavailable_reason()

    def test_eol_reason(self, monkeypatch):
        monkeypatch.setattr(
            "neosqlite.js_runtime.shutil.which", lambda name: "/usr/bin/node"
        )
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda args, **kw: subprocess.CompletedProcess(
                args, 0, stdout="v23.11.0\n", stderr=""
            ),
        )
        reset_node_runtime_cache()
        assert find_node_runtime() is None
        assert "end-of-life" in node_unavailable_reason()

    def test_unpatched_reason(self, monkeypatch):
        monkeypatch.setattr(
            "neosqlite.js_runtime.shutil.which", lambda name: "/usr/bin/node"
        )
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda args, **kw: subprocess.CompletedProcess(
                args, 0, stdout="v24.18.0\n", stderr=""
            ),
        )
        reset_node_runtime_cache()
        assert find_node_runtime() is None
        reason = node_unavailable_reason()
        assert "unpatched" in reason and "24.22.3" in reason

    def test_cache(self, monkeypatch):
        calls = []

        def fake_which(name):
            calls.append(name)
            return "/usr/bin/node"

        monkeypatch.setattr("neosqlite.js_runtime.shutil.which", fake_which)
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda args, **kw: subprocess.CompletedProcess(
                args, 0, stdout="v24.22.3\n", stderr=""
            ),
        )
        reset_node_runtime_cache()
        find_node_runtime()
        find_node_runtime()
        assert calls == ["node"]
