"""Server implementation for MongoDB wire protocol."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import struct
import threading

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


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    handler: NeoSQLiteHandler,
):
    """Handle a single client connection."""
    handler.increment_connections()
    try:
        while True:
            header_bytes = bytearray(16)
            pos = 0
            while pos < 16:
                chunk = await reader.read(16 - pos)
                if not chunk:
                    return
                header_bytes[pos : pos + len(chunk)] = chunk
                pos += len(chunk)
            header = bytes(header_bytes)

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

            if message_length > 16:
                body = bytearray(message_length - 16)
                pos = 0
                remaining = message_length - 16
                while pos < remaining:
                    chunk = await reader.read(remaining - pos)
                    if not chunk:
                        logger.warning(
                            f"Incomplete message: expected {remaining}, got {pos}"
                        )
                        return
                    body[pos : pos + len(chunk)] = chunk
                    pos += len(chunk)
                full_message = header + bytes(body)
            else:
                full_message = header

            match opcode:
                case WireProtocol.OP_MSG:
                    msg = OP_MSG.parse(full_message)

                    is_insert = False
                    for section_type, section_data in msg["sections"]:
                        if (
                            section_type == "body"
                            and isinstance(section_data, dict)
                            and "insert" in section_data
                        ):
                            is_insert = True
                            break

                    has_payload_docs = any(
                        s[0] == "payload_docs" for s in msg["sections"]
                    )

                    try:
                        logger.debug(
                            f"OP_MSG about to handle: is_insert={is_insert}, has_payload={has_payload_docs}"
                        )
                        logger.debug(
                            f"OP_MSG msg['request_id']={msg.get('request_id')}, msg['response_to']={msg.get('response_to')}"
                        )
                        if not is_insert and not has_payload_docs:
                            for section_type, section_data in msg["sections"]:
                                if section_type == "body":
                                    logger.debug(
                                        f"Non-insert command body: {section_data}"
                                    )
                        msg.get("request_id", 0)
                        if is_insert or has_payload_docs:
                            request_id, response_doc = await asyncio.to_thread(
                                handler.handle_insert, msg
                            )
                        else:
                            request_id, response_doc = await asyncio.to_thread(
                                handler.handle_command, msg
                            )
                        logger.debug(
                            f"OP_MSG handled: request_id={request_id}, response_doc={response_doc.get('ok') if isinstance(response_doc, dict) else 'N/A'}"
                        )
                    except Exception as e:
                        logger.error(f"Error handling OP_MSG: {e}")
                        import traceback

                        traceback.print_exc()
                        response_doc = {"ok": 0, "errmsg": str(e)}
                        request_id = msg.get("request_id", 0)

                    try:
                        reply = ResponseBuilder.build_op_msg_reply(
                            request_id=0,
                            response_to=msg.get("request_id", 0),
                            document=response_doc,
                        )
                        writer.write(reply)
                        await writer.drain()
                    except Exception as e:
                        logger.error(f"Error sending response: {e}")

                case WireProtocol.OP_QUERY:
                    msg = OP_QUERY.parse(full_message)
                    orig_request_id = msg["request_id"]
                    try:
                        request_id, docs = await asyncio.to_thread(
                            handler.handle_query, msg
                        )
                        reply = ResponseBuilder.build_reply(
                            request_id, orig_request_id, docs
                        )
                        writer.write(reply)
                        await writer.drain()
                    except Exception as e:
                        logger.error(f"Error handling OP_QUERY: {e}")

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
    ):
        pass
    finally:
        handler.decrement_connections()
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


def handle_client_threaded(
    client_socket: socket.socket,
    handler: NeoSQLiteHandler,
):
    """Handle a single client connection (threaded version)."""
    handler.increment_connections()
    try:
        with client_socket:
            while True:
                header_bytes = bytearray(16)
                pos = 0
                while pos < 16:
                    chunk = client_socket.recv(16 - pos)
                    if not chunk:
                        return
                    header_bytes[pos : pos + len(chunk)] = chunk
                    pos += len(chunk)
                header = bytes(header_bytes)

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

                if message_length > 16:
                    body = bytearray(message_length - 16)
                    pos = 0
                    remaining = message_length - 16
                    while pos < remaining:
                        chunk = client_socket.recv(remaining - pos)
                        if not chunk:
                            logger.warning(
                                f"Incomplete message: expected {remaining}, got {pos}"
                            )
                            return
                        body[pos : pos + len(chunk)] = chunk
                        pos += len(chunk)
                    full_message = header + bytes(body)
                else:
                    full_message = header

                logger.debug(
                    f"Received: len={len(full_message)}, opcode={opcode}"
                )

                if opcode == WireProtocol.OP_MSG:
                    msg = OP_MSG.parse(full_message)

                    is_insert = False
                    for section_type, section_data in msg["sections"]:
                        if (
                            section_type == "body"
                            and isinstance(section_data, dict)
                            and "insert" in section_data
                        ):
                            is_insert = True
                            break

                    if is_insert or any(
                        s[0] == "payload_docs" for s in msg["sections"]
                    ):
                        _, response_doc = (
                            handler.handle_insert(msg)
                            if is_insert
                            else (request_id, {"ok": 0})
                        )
                    else:
                        _, response_doc = handler.handle_command(msg)

                    logger.info(
                        f"Command response: response_keys={list(response_doc.keys()) if isinstance(response_doc, dict) else type(response_doc)}"
                    )
                    response_request_id = _get_next_request_id()
                    reply = ResponseBuilder.build_op_msg_reply(
                        request_id=response_request_id,
                        response_to=request_id,
                        document=response_doc,
                    )
                    logger.info(
                        f"Sending reply: len={len(reply)}, "
                        f"first_40_bytes={list(reply[:40])}"
                    )
                    client_socket.sendall(reply)

                elif opcode == WireProtocol.OP_QUERY:
                    msg = OP_QUERY.parse(full_message)
                    orig_request_id = msg["request_id"]
                    _, docs = handler.handle_query(msg)
                    response_request_id = _get_next_request_id()
                    reply = ResponseBuilder.build_reply(
                        response_request_id, orig_request_id, docs
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

    except (ConnectionResetError, BrokenPipeError, OSError) as e:
        logger.debug(f"Client connection error: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error in client thread: {e}")
    finally:
        handler.decrement_connections()


def run_server_threaded(
    host: str,
    port: int,
    handler: NeoSQLiteHandler,
    use_threading: bool = True,
):
    """Run the MongoDB wire protocol server (threaded or async)."""
    if not use_threading:
        if uvloop is not None:
            uvloop.install()
        asyncio.run(run_server(host, port, handler))
        return

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(SOCKET_BACKLOG)

    logger.info(f"Listening on {host}:{port} (threaded mode)")

    try:
        while True:
            client_socket, addr = server_socket.accept()
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
    """Run the MongoDB wire protocol server."""

    async def handle_client_closure(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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
