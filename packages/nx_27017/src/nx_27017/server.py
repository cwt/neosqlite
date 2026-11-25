"""Server implementation for MongoDB wire protocol."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import struct
import threading
import time
import uuid

from nx_27017.handler import NeoSQLiteHandler
from nx_27017.wire_protocol import (
    MAX_MESSAGE_SIZE_BYTES,
    MESSAGE_HEADER_SIZE,
    OP_MSG,
    OP_QUERY,
    SOCKET_BACKLOG,
    ResponseBuilder,
    WireProtocol,
    _get_next_request_id,
)

try:
    import uvloop
except ImportError:
    uvloop = None  # type: ignore[assignment]

logger = logging.getLogger("nx_27017")

_READ_CHUNK_SIZE = 65536
DEFAULT_FRAME_READ_TIMEOUT = 30.0


def _is_insert_command(msg: dict[str, object]) -> bool:
    """Return True when the OP_MSG body command key is ``insert``.

    Checks the first non-``$`` command key in the body section so non-insert
    commands carrying Kind-2 ``payload_docs`` sections or nested ``insert``
    subfields are never misrouted to ``handle_insert``.
    """
    sections = msg.get("sections", ())
    if not isinstance(sections, list):
        return False
    for section_type, section_data in sections:
        if section_type == "body" and isinstance(section_data, dict):
            for key in section_data:
                if not key.startswith("$"):
                    return key == "insert"
            return False
    return False


async def _read_exact_async(
    reader: asyncio.StreamReader,
    length: int,
    timeout: float | None = DEFAULT_FRAME_READ_TIMEOUT,
    chunk_size: int = _READ_CHUNK_SIZE,
) -> bytes | None:
    """Read exactly ``length`` bytes incrementally in bounded chunks.

    Allocates memory proportional to received bytes rather than the declared
    wire header length, and enforces ``timeout`` across the entire read so
    stalled peers cannot tie up worker resources.
    """
    chunks: list[bytes] = []
    remaining = length
    deadline = time.monotonic() + timeout if timeout is not None else None
    while remaining > 0:
        to_read = min(remaining, chunk_size)
        if deadline is not None:
            time_left = deadline - time.monotonic()
            if time_left <= 0:
                raise TimeoutError("Timed out reading wire frame")
            chunk = await asyncio.wait_for(
                reader.read(to_read), timeout=time_left
            )
        else:
            chunk = await reader.read(to_read)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_exact_sync(
    client_socket: socket.socket,
    length: int,
    timeout: float | None = DEFAULT_FRAME_READ_TIMEOUT,
    chunk_size: int = _READ_CHUNK_SIZE,
) -> bytes | None:
    """Synchronously read exactly ``length`` bytes in bounded chunks."""
    chunks: list[bytes] = []
    remaining = length
    deadline = time.monotonic() + timeout if timeout is not None else None
    try:
        while remaining > 0:
            to_read = min(remaining, chunk_size)
            if deadline is not None:
                time_left = deadline - time.monotonic()
                if time_left <= 0:
                    raise TimeoutError("Timed out reading wire frame")
                client_socket.settimeout(time_left)
            chunk = client_socket.recv(to_read)
            if not chunk:
                return None
            chunks.append(chunk)
            remaining -= len(chunk)
    finally:
        if deadline is not None:
            with contextlib.suppress(OSError):
                client_socket.settimeout(None)
    return b"".join(chunks)


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    handler: NeoSQLiteHandler,
    frame_timeout: float = DEFAULT_FRAME_READ_TIMEOUT,
):
    """Handle a single asynchronous MongoDB wire protocol client connection.

    Enforces ``handler`` connection limits, reads 16-byte wire headers and
    message payloads incrementally in bounded chunks with per-frame timeouts,
    validates message lengths against ``MESSAGE_HEADER_SIZE`` and
    ``MAX_MESSAGE_SIZE_BYTES``, dispatches ``OP_MSG`` (inserts and commands)
    and ``OP_QUERY`` requests via ``asyncio.to_thread``, writes replies to
    ``writer``, and cleans up connection tracking and streams off the event
    loop on disconnect.

    Args:
        reader: Async stream reader for incoming client wire messages.
        writer: Async stream writer for outgoing server replies.
        handler: ``NeoSQLiteHandler`` instance processing client requests.
        frame_timeout: Maximum seconds allowed to receive the remainder of an
            in-progress wire frame once its first bytes arrive.
    """
    if not handler.try_acquire_connection():
        logger.warning("Rejecting client connection: max connections reached")
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        return
    conn_id = str(uuid.uuid4())
    try:
        while True:
            first_chunk = await reader.read(MESSAGE_HEADER_SIZE)
            if not first_chunk:
                return
            if len(first_chunk) < MESSAGE_HEADER_SIZE:
                rest_header = await _read_exact_async(
                    reader,
                    MESSAGE_HEADER_SIZE - len(first_chunk),
                    timeout=frame_timeout,
                )
                if rest_header is None:
                    return
                header = first_chunk + rest_header
            else:
                header = first_chunk

            message_length = struct.unpack("<i", header[0:4])[0]
            request_id = struct.unpack("<i", header[4:8])[0]
            struct.unpack("<i", header[8:12])[0]
            opcode = struct.unpack("<i", header[12:16])[0]

            if (
                message_length < MESSAGE_HEADER_SIZE
                or message_length > MAX_MESSAGE_SIZE_BYTES
            ):
                logger.warning(f"Invalid message length: {message_length}")
                return

            if message_length > MESSAGE_HEADER_SIZE:
                body = await _read_exact_async(
                    reader,
                    message_length - MESSAGE_HEADER_SIZE,
                    timeout=frame_timeout,
                )
                if body is None:
                    logger.warning(
                        f"Incomplete message: expected "
                        f"{message_length - MESSAGE_HEADER_SIZE} body bytes"
                    )
                    return
                full_message = header + body
            else:
                full_message = header

            match opcode:
                case WireProtocol.OP_MSG:
                    orig_request_id = request_id
                    more_to_come = False
                    try:
                        msg = OP_MSG.parse(full_message)
                        msg["_conn_id"] = conn_id
                        orig_request_id = msg.get("request_id", orig_request_id)
                        more_to_come = bool(msg.get("more_to_come", False))

                        if _is_insert_command(msg):
                            request_id, response_doc = await asyncio.to_thread(
                                handler.handle_insert, msg
                            )
                        else:
                            request_id, response_doc = await asyncio.to_thread(
                                handler.handle_command, msg
                            )
                    except Exception as e:
                        logger.exception("Error handling OP_MSG")
                        response_doc = {"ok": 0, "errmsg": str(e)}
                        request_id = orig_request_id

                    if not more_to_come:
                        try:
                            reply = ResponseBuilder.build_op_msg_reply(
                                request_id=_get_next_request_id(),
                                response_to=request_id,
                                document=response_doc,
                            )
                            writer.write(reply)
                            await writer.drain()
                        except Exception as e:
                            logger.error(f"Error sending response: {e}")

                case WireProtocol.OP_QUERY:
                    orig_request_id = request_id
                    try:
                        msg = OP_QUERY.parse(full_message)
                        msg["_conn_id"] = conn_id
                        orig_request_id = msg.get("request_id", orig_request_id)
                        reply_req_id, docs = await asyncio.to_thread(
                            handler.handle_query, msg
                        )
                        reply = ResponseBuilder.build_reply(
                            reply_req_id, orig_request_id, docs
                        )
                    except Exception as e:
                        logger.error(f"Error handling OP_QUERY: {e}")
                        reply = ResponseBuilder.build_reply(
                            _get_next_request_id(),
                            orig_request_id,
                            [{"ok": 0, "$err": str(e), "errmsg": str(e)}],
                            flags=2,
                        )
                    try:
                        writer.write(reply)
                        await writer.drain()
                    except Exception as e:
                        logger.error(f"Error sending OP_QUERY response: {e}")

                case _:
                    error_reply = ResponseBuilder.build_op_msg_reply(
                        request_id=0,
                        response_to=request_id,
                        document={
                            "ok": 0,
                            "errmsg": f"Unsupported opcode: {opcode}",
                        },
                    )
                    writer.write(error_reply)
                    await writer.drain()

    except (
        ConnectionResetError,
        BrokenPipeError,
        asyncio.IncompleteReadError,
        TimeoutError,
    ):
        pass
    finally:
        handler.decrement_connections()
        await asyncio.to_thread(handler.close_streams_for_connection, conn_id)
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


def handle_client_threaded(
    client_socket: socket.socket,
    handler: NeoSQLiteHandler,
    frame_timeout: float = DEFAULT_FRAME_READ_TIMEOUT,
):
    """Handle a single client connection on a dedicated OS thread.

    Enforces ``handler`` connection limits, reads wire protocol frames
    incrementally in bounded chunks with per-frame timeouts, validates
    message sizes, dispatches ``OP_MSG`` and ``OP_QUERY`` requests on
    ``handler``, sends replies via ``sendall``, and decrements connection
    tracking and closes open cursors/streams when the client disconnects.

    Args:
        client_socket: Connected client TCP socket.
        handler: ``NeoSQLiteHandler`` instance processing client requests.
        frame_timeout: Maximum seconds allowed to receive the remainder of an
            in-progress wire frame once its first bytes arrive.
    """
    if not handler.try_acquire_connection():
        logger.warning("Rejecting threaded connection: max connections reached")
        with contextlib.suppress(OSError):
            client_socket.close()
        return
    conn_id = str(uuid.uuid4())
    try:
        with client_socket:
            while True:
                first_chunk = client_socket.recv(MESSAGE_HEADER_SIZE)
                if not first_chunk:
                    return
                if len(first_chunk) < MESSAGE_HEADER_SIZE:
                    rest_header = _read_exact_sync(
                        client_socket,
                        MESSAGE_HEADER_SIZE - len(first_chunk),
                        timeout=frame_timeout,
                    )
                    if rest_header is None:
                        return
                    header = first_chunk + rest_header
                else:
                    header = first_chunk

                message_length = struct.unpack("<i", header[0:4])[0]
                request_id = struct.unpack("<i", header[4:8])[0]
                struct.unpack("<i", header[8:12])[0]
                opcode = struct.unpack("<i", header[12:16])[0]

                if (
                    message_length < MESSAGE_HEADER_SIZE
                    or message_length > MAX_MESSAGE_SIZE_BYTES
                ):
                    logger.warning(f"Invalid message length: {message_length}")
                    return

                if message_length > MESSAGE_HEADER_SIZE:
                    body = _read_exact_sync(
                        client_socket,
                        message_length - MESSAGE_HEADER_SIZE,
                        timeout=frame_timeout,
                    )
                    if body is None:
                        logger.warning(
                            f"Incomplete message: expected "
                            f"{message_length - MESSAGE_HEADER_SIZE} body bytes"
                        )
                        return
                    full_message = header + body
                else:
                    full_message = header

                logger.debug(
                    f"Received: len={len(full_message)}, opcode={opcode}"
                )

                if opcode == WireProtocol.OP_MSG:
                    orig_request_id = request_id
                    more_to_come = False
                    try:
                        msg = OP_MSG.parse(full_message)
                        msg["_conn_id"] = conn_id
                        orig_request_id = msg.get("request_id", orig_request_id)
                        more_to_come = bool(msg.get("more_to_come", False))

                        if _is_insert_command(msg):
                            request_id, response_doc = handler.handle_insert(
                                msg
                            )
                        else:
                            request_id, response_doc = handler.handle_command(
                                msg
                            )
                    except Exception as e:
                        logger.exception(
                            f"Error handling OP_MSG in threaded client: {e}"
                        )
                        response_doc = {"ok": 0, "errmsg": str(e)}
                        request_id = orig_request_id

                    if not more_to_come:
                        logger.info(
                            f"Command response: response_keys={list(response_doc.keys()) if isinstance(response_doc, dict) else type(response_doc)}"
                        )
                        response_request_id = _get_next_request_id()
                        reply = ResponseBuilder.build_op_msg_reply(
                            request_id=response_request_id,
                            response_to=request_id,
                            document=response_doc,
                        )
                        client_socket.sendall(reply)

                elif opcode == WireProtocol.OP_QUERY:
                    orig_request_id = request_id
                    try:
                        msg = OP_QUERY.parse(full_message)
                        msg["_conn_id"] = conn_id
                        orig_request_id = msg.get("request_id", orig_request_id)
                        _, docs = handler.handle_query(msg)
                        response_request_id = _get_next_request_id()
                        reply = ResponseBuilder.build_reply(
                            response_request_id, orig_request_id, docs
                        )
                    except Exception as e:
                        logger.error(f"Error handling OP_QUERY: {e}")
                        reply = ResponseBuilder.build_reply(
                            _get_next_request_id(),
                            orig_request_id,
                            [{"ok": 0, "$err": str(e), "errmsg": str(e)}],
                            flags=2,
                        )
                    client_socket.sendall(reply)

                else:
                    error_reply = ResponseBuilder.build_op_msg_reply(
                        request_id=0,
                        response_to=request_id,
                        document={
                            "ok": 0,
                            "errmsg": f"Unsupported opcode: {opcode}",
                        },
                    )
                    client_socket.sendall(error_reply)

    except (ConnectionResetError, BrokenPipeError, TimeoutError, OSError) as e:
        logger.debug(f"Client connection error: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error in client thread: {e}")
    finally:
        handler.decrement_connections()
        handler.close_streams_for_connection(conn_id)


def run_server_threaded(
    host: str,
    port: int,
    handler: NeoSQLiteHandler,
    use_threading: bool = True,
):
    """Run the MongoDB wire protocol server in threaded or async mode.

    When ``use_threading`` is False, starts ``run_server`` under
    ``asyncio.run`` (using ``uvloop.new_event_loop`` when available).
    Otherwise binds a reusable IPv4 TCP socket on ``(host, port)`` and spawns
    daemon threads running ``handle_client_threaded`` per accepted client
    until interrupted by ``KeyboardInterrupt``.

    Args:
        host: Hostname or IPv4 address to bind the listening socket to.
        port: TCP port number to listen on.
        handler: ``NeoSQLiteHandler`` instance serving incoming connections.
        use_threading: If True, run the multi-threaded socket loop; if False,
            delegate to the async ``asyncio`` server.
    """
    if not use_threading:
        asyncio.run(
            run_server(host, port, handler),
            loop_factory=uvloop.new_event_loop if uvloop is not None else None,
        )
        return

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(SOCKET_BACKLOG)

    logger.info(f"Listening on {host}:{port} (threaded mode)")

    try:
        while True:
            try:
                client_socket, addr = server_socket.accept()
            except OSError as e:
                if (
                    getattr(server_socket, "_closed", False)
                    or server_socket.fileno() == -1
                ):
                    break
                logger.warning(f"Error accepting connection: {e}")
                time.sleep(0.05)
                continue
            logger.info(f"Accepted connection from {addr}")

            client_thread = threading.Thread(
                target=handle_client_threaded,
                args=(client_socket, handler),
                daemon=True,
            )
            client_thread.start()

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        server_socket.close()


async def run_server(host: str, port: int, handler: NeoSQLiteHandler):
    """Run the asynchronous MongoDB wire protocol server.

    Starts an ``asyncio`` TCP server on ``(host, port)`` that delegates each
    accepted connection to ``handle_client`` and runs until stopped.

    Args:
        host: Hostname or IP address to bind the server to.
        port: TCP port number to listen on.
        handler: ``NeoSQLiteHandler`` instance serving incoming connections.
    """

    async def handle_client_closure(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Forward an accepted client stream pair to ``handle_client``.

        Args:
            reader: Async stream reader for the accepted connection.
            writer: Async stream writer for the accepted connection.
        """
        await handle_client(reader, writer, handler)

    server = await asyncio.start_server(
        handle_client_closure,
        host,
        port,
    )

    addr = server.sockets[0].getsockname()
    logger.info(f"Listening on {addr[0]}:{addr[1]}")

    async with server:
        await server.serve_forever()
