import contextlib
import importlib.util
import io
import json
import socket
import time
import logging
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from coordination.bus import FileBus

ADAPTER = Path(__file__).resolve().parent.parent / "adapter" / "adapter.py"


def _load_adapter():
    spec = importlib.util.spec_from_file_location("win35_adapter", ADAPTER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def adapter_server(tmp_path: Path):
    adapter = _load_adapter()
    bus = FileBus(tmp_path / "store")
    server = adapter.BusServer(("127.0.0.1", 0), adapter.BusAdapterHandler, bus, {}, None)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield bus, f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def _get(url, headers=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read())


def test_tokens_never_logged(adapter_server):
    bus, base = adapter_server
    ident, token = bus.register(agent_name="a", device_id="d", project_id="p")
    records = io.StringIO()
    handler = logging.StreamHandler(records)
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    stderr = io.StringIO()
    try:
        with contextlib.redirect_stderr(stderr):
            assert _get(f"{base}/v1/inbox?identity_id={ident.identity_id}&token={token}")[0] == 200
            assert _get(f"{base}/v1/inbox?identity_id={ident.identity_id}&token=wrong-secret")[0] == 400
            assert _get(f"{base}/v1/inbox?identity_id=nobody&token=unknown-secret")[0] == 400
            inbox = f"{base}/v1/inbox?identity_id={ident.identity_id}"
            assert _get(inbox, {"Authorization": f"Bearer {token}"})[0] == 200
            assert _get(inbox, {"Authorization": "Bearer header-secret"})[0] == 400
            send = {"sender_id": ident.identity_id, "recipient_id": ident.identity_id, "body": "x"}
            assert _get(f"{base}/v1/send", body={**send, "token": token})[0] == 200
            assert _get(f"{base}/v1/send", body={**send, "token": "post-secret"})[0] == 400
            ack = {"identity_id": ident.identity_id, "message_id": "missing"}
            assert _get(f"{base}/v1/ack", body={**ack, "token": "ack-secret"})[0] == 400
    finally:
        root.removeHandler(handler)
    output = records.getvalue() + stderr.getvalue()
    assert "/v1/inbox" in output
    for secret in (token, "wrong-secret", "unknown-secret", "header-secret",
                   "post-secret", "ack-secret", "token="):
        assert secret not in output


def test_bearer_header_auth(adapter_server):
    bus, base = adapter_server
    ident, token = bus.register(agent_name="a", device_id="d", project_id="p")
    url = f"{base}/v1/inbox?identity_id={ident.identity_id}"
    assert _get(url, {"Authorization": f"Bearer {token}"}) == (200, {"status": "ok", "messages": []})
    assert _get(url, {"Authorization": f"bearer {token}"})[0] == 200
    assert _get(url, {"Authorization": f"Bearer\t {token}"})[0] == 200
    assert _get(url, {"Authorization": "Bearer"})[0] == 400
    assert _get(url, {"Authorization": "Bearer wrong"})[0] == 400
    assert _get(url, {"Authorization": f"Basic {token}"})[0] == 400
    assert _get(url)[0] == 400


def test_malformed_request_line_does_not_reflect_or_log_token(adapter_server):
    _, base = adapter_server
    host, port = base.rsplit("/", 1)[-1].split(":")
    records = io.StringIO()
    handler = logging.StreamHandler(records)
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    stderr = io.StringIO()
    responses = []
    try:
        with contextlib.redirect_stderr(stderr):
            for raw in (b"GET /v1/inbox?token=line-secret extra junk HTTP/1.1\r\n\r\n",
                        b"GET /v1/inbox?token=version-secret HTTP/9.9\r\n\r\n"):
                with socket.create_connection((host, int(port))) as s:
                    s.sendall(raw)
                    chunks = []
                    while chunk := s.recv(65536):
                        chunks.append(chunk)
                    responses.append(b"".join(chunks))
            time.sleep(0.1)
    finally:
        root.removeHandler(handler)
    assert responses[0].startswith(b"HTTP/1.0 400 Bad Request")
    output = records.getvalue() + stderr.getvalue()
    assert "Traceback" not in output
    for secret in ("line-secret", "version-secret"):
        assert secret not in output
        assert all(secret.encode() not in r for r in responses)
