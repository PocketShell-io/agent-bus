import contextlib
import importlib.util
import io
import json
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


def _get(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
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
    finally:
        root.removeHandler(handler)
    output = records.getvalue() + stderr.getvalue()
    assert "/v1/inbox" in output
    for secret in (token, "wrong-secret", "unknown-secret", "token="):
        assert secret not in output


def test_bearer_header_auth(adapter_server):
    bus, base = adapter_server
    ident, token = bus.register(agent_name="a", device_id="d", project_id="p")
    url = f"{base}/v1/inbox?identity_id={ident.identity_id}"
    assert _get(url, {"Authorization": f"Bearer {token}"}) == (200, {"status": "ok", "messages": []})
    assert _get(url, {"Authorization": "Bearer wrong"})[0] == 400
    assert _get(url)[0] == 400
