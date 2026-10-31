#!/usr/bin/env python3
"""Loopback-only native networking peer. Requires websockets==15.0.1.

Routes: /body, /invalid, /echo, /protocol, /fragment, /utf8,
/slow, /flood, /floodcancel, /stats. No CA is installed.
Instrumentation deliberately uses websockets' Sans-I/O Frame serializer and
ServerConnection.process_event: its RFC6455 decoder validates client masking,
control-frame lengths and opcodes before the observer sees a Pong. We bypass
ping() only to avoid retaining one Future per unanswered Ping. pause_reading()
is on the actual transport; there is no second socket reader.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import websockets
from websockets.asyncio.server import serve, ServerConnection
from websockets.asyncio.client import connect
from websockets.frames import Frame, OP_TEXT, OP_CONT, OP_PING, OP_PONG

COUNTS = {"connections": 0, "pongs": 0, "final_pong": 0, "utf8_close": 0,
          "flood_sent": 0, "slow_sent": 0, "flood_resumed": 0}
FINAL = b"FINAL:" + b"z" * 119

class Observed(ServerConnection):
    def connection_made(self, transport):
        COUNTS["connections"] += 1
        super().connection_made(transport)

        self.final_pong = False
    def process_event(self, event):
        if isinstance(event, Frame) and event.opcode == OP_PONG:
            COUNTS["pongs"] += 1
            if event.data == FINAL:
                COUNTS["final_pong"] += 1
                self.final_pong = True
        super().process_event(event)

async def request(connection, request):
    path = request.path.split("?", 1)[0]
    if path in ("/body", "/invalid", "/stats"):
        body = json.dumps(COUNTS) if path == "/stats" else "download-payload"
        return connection.respond(200, body)
    return None

def response(connection, request, response):
    if request.path == "/protocol" and response.status_code == 101:
        # Deliberate server protocol violation, not the library's negotiation.
        response.headers["Sec-WebSocket-Protocol"] = "CHAT"
    return response

async def handler(ws):
    path = ws.request.path
    try:
        if path == "/fragment":
            # U+2603 split inside its three-byte UTF-8 encoding.
            ws.transport.write(Frame(OP_TEXT, b"split \xe2", fin=False).serialize(mask=False))
            ws.transport.write(Frame(OP_CONT, b"\x98\x83", fin=True).serialize(mask=False))
            await ws.wait_closed()
        elif path == "/utf8":
            ws.transport.write(Frame(OP_TEXT, b"\xff").serialize(mask=False))
            await ws.wait_closed()
            if ws.protocol.close_rcvd and ws.protocol.close_rcvd.code == 1007:
                COUNTS["utf8_close"] += 1
        elif path == "/slow":
            payload = b"s" * 65536
            for _ in range(4096):
                await ws.send(payload)
                COUNTS["slow_sent"] += 1
            await ws.send("drained")
            await ws.wait_closed()
        elif path in ("/flood", "/floodcancel"):
            ws.transport.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            ws.transport.pause_reading()
            try:
                # 8 MiB of distinct control payloads; ~16 MiB for cancel case.
                limit = 65536 if path == "/flood" else 131072
                for i in range(limit):
                    if ws.transport.is_closing():
                        return
                    payload = i.to_bytes(8, "big") + b"p" * 117
                    ws.transport.write(Frame(OP_PING, payload).serialize(mask=False))
                    COUNTS["flood_sent"] += 1
                    if i % 64 == 63:
                        await ws.drain()
                        await asyncio.sleep(0.001)
                ws.transport.write(Frame(OP_PING, FINAL).serialize(mask=False))
                await ws.drain()
                await asyncio.sleep(3)
            finally:
                if not ws.transport.is_closing():
                    ws.transport.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)
                    ws.transport.resume_reading()
                    COUNTS["flood_resumed"] += 1
            if path == "/flood":
                async with asyncio.timeout(60):
                    while not ws.final_pong:
                        await asyncio.sleep(0.02)
                await ws.send("final-pong-observed")
                await ws.close()
            else:
                await ws.wait_closed()
        else:
            async for message in ws:
                await ws.send(message)
    except (websockets.ConnectionClosed, ConnectionError, TimeoutError):
        pass

async def smoke(http, tls, cert):
    import urllib.request
    body = await asyncio.to_thread(lambda: urllib.request.urlopen(f"http://127.0.0.1:{http}/body").read())
    assert body == b"download-payload", repr(body)
    async with connect(f"ws://127.0.0.1:{http}/echo", proxy=None) as ws:
        for value in ("snow ☃", b"\x00\xff"):
            await ws.send(value)
            assert await ws.recv() == value
        await (await ws.ping(b"host-ping"))
    async with connect(f"ws://127.0.0.1:{http}/echo", proxy=None) as ws:
        ws.transport.write(Frame(OP_PING, b"unmasked").serialize(mask=False))
        try:
            await ws.recv()
            raise AssertionError("strict decoder accepted unmasked client frame")
        except websockets.ConnectionClosedError as error:
            assert error.rcvd.code == 1002
    # Explicit local test-client trust only; never changes guest/system trust.
    context = ssl.create_default_context(cafile=str(cert))
    async with connect(f"wss://localhost:{tls}/echo", ssl=context, proxy=None) as ws:
        await ws.send("TLS")
        assert await ws.recv() == "TLS"
    try:
        async with connect(f"wss://localhost:{tls}/echo", proxy=None):
            raise AssertionError("self-signed certificate trusted by default")
    except ssl.SSLCertVerificationError:
        pass
    try:
        async with connect(f"ws://127.0.0.1:{http}/invalid", proxy=None):
            raise AssertionError("HTTP 200 accepted as upgrade")
    except websockets.exceptions.InvalidStatus as error:
        assert error.response.status_code == 200
    async with connect(f"ws://127.0.0.1:{http}/fragment", proxy=None) as ws:
        assert await ws.recv() == "split ☃"
    async with connect(f"ws://127.0.0.1:{http}/utf8", proxy=None) as ws:
        try:
            await ws.recv()
            raise AssertionError("invalid UTF-8 delivered")
        except websockets.ConnectionClosedError as error:
            assert error.sent.code == 1007
    try:
        async with connect(f"ws://127.0.0.1:{http}/protocol", subprotocols=["chat"], proxy=None):
            raise AssertionError("unoffered protocol accepted")
    except websockets.exceptions.NegotiationError:
        pass
    async with connect(f"ws://127.0.0.1:{http}/flood", proxy=None, ping_interval=None) as ws:
        assert await ws.recv() == "final-pong-observed"
    async with connect(f"ws://127.0.0.1:{http}/slow", proxy=None, max_queue=1) as ws:
        await asyncio.sleep(0.2)
        for _ in range(4096):
            assert await ws.recv() == b"s" * 65536
        assert await ws.recv() == "drained"
        await (await ws.ping(b"after-drain"))
    async with connect(f"ws://127.0.0.1:{http}/floodcancel", proxy=None, ping_interval=None) as ws:
        await asyncio.sleep(0.5)
        await ws.close()
    assert COUNTS["final_pong"] and COUNTS["utf8_close"]
    print("HOST SELFSMOKE PASS " + json.dumps(COUNTS), flush=True)

async def main(args):
    assert websockets.__version__ == "15.0.1"
    state = Path(args.state).resolve()
    state.mkdir(parents=True, exist_ok=True)
    key, cert = state / "localhost.key", state / "localhost.pem"
    if not cert.exists():
        old = os.umask(0o077)
        try:
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                            "-subj", "/CN=localhost", "-addext", "subjectAltName=IP:127.0.0.1,DNS:localhost",
                            "-keyout", str(key), "-out", str(cert)], check=True, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
        finally:
            os.umask(old)
    assert key.stat().st_mode & 0o777 == 0o600
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    options = dict(process_request=request, process_response=response, create_connection=Observed,
                   ping_interval=None, max_queue=1, write_limit=32768, close_timeout=3)
    async with serve(handler, "127.0.0.1", args.http, **options), serve(handler, "127.0.0.1", args.tls, ssl=context, **options):
        print(f"READY networking-peer-r89 http={args.http} tls={args.tls} websockets={websockets.__version__}", flush=True)
        if args.self_smoke:
            await smoke(args.http, args.tls, cert)
        else:
            await asyncio.Future()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--http", type=int, default=50591)
    parser.add_argument("--tls", type=int, default=50592)
    parser.add_argument("--self-smoke", action="store_true", help="finite real HTTP/WS/WSS protocol checks, then exit")
    asyncio.run(main(parser.parse_args()))
