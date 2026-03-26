"""Tests for daemon functions."""

import os
from unittest.mock import MagicMock, patch

import pytest


class TestDaemonFunctions:
    """Test daemon-related functions."""

    def test_pid_file_operations(self, tmp_path):
        """Test PID file write and read."""
        from nx_27017.nx_27017 import get_pid, write_pid_file

        pid_file = tmp_path / "test.pid"
        result = write_pid_file(str(pid_file))
        assert result is True
        pid = get_pid(str(pid_file))
        assert pid == os.getpid()

    def test_pid_file_not_exists(self, tmp_path):
        """Test get_pid when file doesn't exist."""
        from nx_27017.nx_27017 import get_pid

        pid = get_pid(str(tmp_path / "nonexistent.pid"))
        assert pid is None

    def test_pid_file_invalid_content(self, tmp_path):
        """Test get_pid with invalid content."""
        from nx_27017.nx_27017 import get_pid

        pid_file = tmp_path / "invalid.pid"
        pid_file.write_text("not-a-number")
        pid = get_pid(str(pid_file))
        assert pid is None

    def test_stop_daemon_no_pid_file(self, tmp_path):
        """Test stop_daemon when no PID file exists."""
        from nx_27017.nx_27017 import stop_daemon

        result = stop_daemon(str(tmp_path / "nonexistent.pid"))
        assert result == 1

    def test_stop_daemon_invalid_pid(self, tmp_path):
        """Test stop_daemon with invalid PID."""
        from nx_27017.nx_27017 import stop_daemon

        pid_file = tmp_path / "invalid.pid"
        pid_file.write_text("999999")
        result = stop_daemon(str(pid_file))
        assert result in (0, 1)

    def test_check_status_not_running(self, tmp_path):
        """Test check_status when not running."""
        from nx_27017.nx_27017 import check_status

        result = check_status(str(tmp_path / "nonexistent.pid"))
        assert result == 1

    def test_daemonize(self):
        """Test daemonize function (basic coverage)."""
        from nx_27017.nx_27017 import daemonize

        with patch("os.fork") as mock_fork:
            mock_fork.side_effect = OSError("Cannot fork")
            with pytest.raises(SystemExit):
                daemonize()

    def test_run_as_daemon_already_running(self, tmp_path):
        """Test run_as_daemon when already running."""
        from nx_27017.nx_27017 import run_as_daemon

        pid_file = tmp_path / "test.pid"
        pid_file.write_text(str(os.getpid()))

        args = MagicMock()
        args.pid_file = str(pid_file)
        args.db_path = ":memory:"
        args.host = "127.0.0.1"
        args.port = 27017
        args.journal_mode = "WAL"
        args.log_level = "INFO"
        args.fts5_tokenizers = None
        args.threaded = False

        with pytest.raises(SystemExit):
            run_as_daemon(args)

    def test_run_as_daemon_relative_path(self, tmp_path):
        """Test run_as_daemon with relative db path."""
        from nx_27017.nx_27017 import run_as_daemon

        pid_file = tmp_path / "test.pid"

        args = MagicMock()
        args.pid_file = str(pid_file)
        args.db_path = "relative.db"
        args.host = "127.0.0.1"
        args.port = 27017
        args.journal_mode = "WAL"
        args.log_level = "INFO"
        args.fts5_tokenizers = None
        args.threaded = False
        args.log_file = str(tmp_path / "test.log")

        with patch("os.fork") as mock_fork:
            mock_fork.side_effect = OSError("Cannot fork")
            with pytest.raises(SystemExit):
                run_as_daemon(args)
