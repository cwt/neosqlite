"""Safe detection of an external Node.js executable for JS operators.

NeoSQLite evaluates server-side JavaScript in a sandboxed out-of-process
worker rather than embedding a JS engine. Only patched, supported Node.js
lines are accepted:

- 22.x >= 22.23.5 (Maintenance LTS)
- 24.x >= 24.22.3 (Active LTS)
- 26.x >= 26.13.1 (Current)

EOL lines (18, 20, odd-numbered 23/25, and anything older) are rejected, as
are unpatched releases of active lines. These minimums come from the 2026-11-11
Node.js security release covering CVE-2026-111497 (Node2Shell Brotli RCE) and
the November 2026 HTTP/2 and TLS fixes.
"""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from functools import lru_cache

# Minimum patched patch-level per supported major line.
_SUPPORTED_LINES: dict[int, tuple[int, int, int]] = {
    22: (22, 23, 5),
    24: (24, 22, 3),
    26: (26, 13, 1),
}

_VERSION_TIMEOUT = 3.0


@dataclass(frozen=True)
class NodeRuntime:
    """A vetted Node.js executable."""

    path: str
    version: tuple[int, int, int]

    @property
    def version_string(self) -> str:
        """Return the version as a dotted string."""
        return ".".join(str(part) for part in self.version)


def parse_node_version(output: str) -> tuple[int, int, int] | None:
    """Parse the output of ``node --version``.

    Args:
        output: Raw version text, with or without a leading ``v``.

    Returns:
        tuple[int, int, int] | None: The numeric version, or ``None`` when the
        output is not a recognizable Node.js version.
    """
    text = output.strip()
    text = text.removeprefix("v")
    parts = text.split(".")
    if len(parts) < 2:
        return None
    try:
        numbers = [int(part.split("-")[0]) for part in parts[:3]]
    except ValueError:
        return None
    while len(numbers) < 3:
        numbers.append(0)
    return numbers[0], numbers[1], numbers[2]


def is_supported_node_version(version: tuple[int, int, int]) -> bool:
    """Return whether a Node.js version is supported and patched."""
    minimum = _SUPPORTED_LINES.get(version[0])
    return minimum is not None and version >= minimum


def probe_node(path: str) -> NodeRuntime | None:
    """Probe a candidate executable and return it when supported."""
    try:
        completed = subprocess.run(
            [path, "--version"],
            capture_output=True,
            text=True,
            timeout=_VERSION_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    version = parse_node_version(completed.stdout)
    if version is None or not is_supported_node_version(version):
        return None
    return NodeRuntime(path=path, version=version)


@lru_cache(maxsize=1)
def find_node_runtime() -> NodeRuntime | None:
    """Locate a supported Node.js executable on ``PATH`` (cached)."""
    path = shutil.which("node")
    if path is None:
        return None
    return probe_node(path)


def reset_node_runtime_cache() -> None:
    """Clear the cached result (primarily for tests)."""
    find_node_runtime.cache_clear()


def node_unavailable_reason() -> str:
    """Explain why JavaScript operators cannot be used."""
    supported = find_node_runtime()
    if supported is not None:
        return f"available (Node.js {supported.version_string})"
    path = shutil.which("node")
    if path is None:
        return "Node.js executable not found on PATH"
    try:
        completed = subprocess.run(
            [path, "--version"],
            capture_output=True,
            text=True,
            timeout=_VERSION_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return f"Node.js probe failed: {exc}"
    version = parse_node_version(completed.stdout)
    if version is None:
        return (
            f"Unrecognized Node.js version output: {completed.stdout.strip()!r}"
        )
    major = version[0]
    if major not in _SUPPORTED_LINES:
        return (
            f"Node.js {version[0]}.{version[1]}.{version[2]} is end-of-life; "
            "use 22.x, 24.x, or 26.x"
        )
    minimum = _SUPPORTED_LINES[major]
    return (
        f"Node.js {version[0]}.{version[1]}.{version[2]} is unpatched; "
        f"requires >= {'.'.join(str(p) for p in minimum)}"
    )
