"""Tests for server.py accept error handling."""

from unittest.mock import MagicMock, patch

from nx_27017.server import run_server_threaded


def test_threaded_server_accept_oserror_recovery():
    """Verify that OSError during accept() does not crash the threaded server."""
    mock_socket = MagicMock()
    mock_socket.fileno.side_effect = [10, 10, -1]
    mock_socket._closed = False

    client_sock = MagicMock()
    mock_socket.accept.side_effect = [
        OSError(24, "Too many open files"),
        (client_sock, ("127.0.0.1", 54321)),
        KeyboardInterrupt(),
    ]

    mock_handler = MagicMock()

    with patch("socket.socket", return_value=mock_socket):
        with patch("time.sleep") as mock_sleep:
            with patch("threading.Thread") as mock_thread:
                run_server_threaded(
                    "127.0.0.1", 27017, mock_handler, use_threading=True
                )
                assert mock_sleep.called
                assert mock_thread.called


def test_async_server_uses_loop_factory():
    """Verify non-threaded server passes uvloop.new_event_loop or None via loop_factory."""
    mock_handler = MagicMock()
    mock_coro = MagicMock()

    with patch("nx_27017.server.run_server", MagicMock(return_value=mock_coro)):
        fake_uvloop = MagicMock()
        with (
            patch("nx_27017.server.uvloop", fake_uvloop),
            patch("asyncio.run") as mock_asyncio_run,
        ):
            run_server_threaded(
                "127.0.0.1", 27017, mock_handler, use_threading=False
            )
            mock_asyncio_run.assert_called_once_with(
                mock_coro, loop_factory=fake_uvloop.new_event_loop
            )
            fake_uvloop.install.assert_not_called()

        with (
            patch("nx_27017.server.uvloop", None),
            patch("asyncio.run") as mock_asyncio_run,
        ):
            run_server_threaded(
                "127.0.0.1", 27017, mock_handler, use_threading=False
            )
            mock_asyncio_run.assert_called_once_with(
                mock_coro, loop_factory=None
            )
