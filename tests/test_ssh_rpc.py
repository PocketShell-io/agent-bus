"""Component test suite for typed SSH FileBus RPC client and envelope.

Tests:
- NamespacedId structured serialization and URI-safe roundtrip (Windows drive letters, slashes).
- RpcRequest and RpcResponse serialization.
- Full FileBus lifecycle over RPC (enroll, send, inbox, ack, reply).
- Typed replay and idempotency key handling.
- Negative tests: SSH connection failure (exit 255 -> TransportError).
- Negative tests: SSH timeout -> TransportTimeout (decoupled chaining from None).
- Negative tests: Corrupted banner / non-JSON framing -> FramingError (decoupled chaining from None).
- Negative tests: Authentication failure -> AuthError.
- Zero credential leakage in command argv.
- C2162: Host option injection defense (rejects '-' prefix, verifies '--' before host).
- C2162 / C2164: Remote shell escaping (shlex.quote) for POSIX login shells.
- C2162 / C2166: Strict correlation check (request_id_mismatch -> FramingError from None).
- C2162: Strict boolean check (non-bool 'ok' -> FramingError).
- C2162: Strict exit code check (exit != 0 with valid JSON -> TransportError).
- C2166: Fail-closed omission (complete omission of stdout/stderr from exception message and attributes).
- C2166: Decoupled exception chaining (raise from None prevents traceback leakage).
- C2166: Mandatory SSH options (-o BatchMode=yes, -o StrictHostKeyChecking=yes, rejects accept-new/no).
"""

from __future__ import annotations

import json
from pathlib import Path
import shlex
import subprocess
import sys
import pytest

from coordination.envelope import (
    NamespacedId,
    RpcRequest,
    RpcResponse,
    TransportState,
    new_idempotency_key,
    new_request_id,
)
from coordination.errors import (
    AuthError,
    FramingError,
    IdempotencyConflict,
    TransportError,
    TransportTimeout,
)
from coordination.ssh_rpc import SshFileBusClient, _redact


def test_namespaced_id_serialization_and_uri_roundtrip():
    """Verify NamespacedId serialization and URI encoding across Unix and Windows path variants."""
    # Case 1: Standard Unix path
    nid_unix = NamespacedId(
        host="hetzner-node-1",
        store_path="/home/alexey/.local/agent_bus",
        agent_tag="worker-zcode",
        session_id="sess-42",
        task_id="task-100",
    )
    d = nid_unix.to_dict()
    assert d["host"] == "hetzner-node-1"
    assert d["store_path"] == "/home/alexey/.local/agent_bus"
    assert NamespacedId.from_dict(d) == nid_unix

    uri = nid_unix.encode_uri()
    assert uri.startswith("nid:")
    assert "%2F" in uri
    decoded = NamespacedId.decode_uri(uri)
    assert decoded == nid_unix
    assert NamespacedId.from_uri(nid_unix.to_uri()) == nid_unix

    # Case 2: Windows drive letter and backslashes
    nid_win = NamespacedId(
        host="windows-dev-box",
        store_path=r"C:\Users\Alexey\AppData\Local\agent_bus",
        agent_tag="win-agent",
        session_id="sess-win",
        task_id="task-win-01",
    )
    win_uri = nid_win.encode_uri()
    assert "C%3A%5C" in win_uri
    assert NamespacedId.from_uri(win_uri) == nid_win

    # Case 3: Invalid URI scheme rejected
    with pytest.raises(ValueError, match="Invalid NamespacedId URI scheme"):
        NamespacedId.decode_uri("http://example.com/foo")

    with pytest.raises(ValueError, match="Expected 5 parts"):
        NamespacedId.decode_uri("nid:too:few:parts")


def test_rpc_request_response_serialization():
    """Verify RpcRequest and RpcResponse serialization."""
    req = RpcRequest(
        op="send",
        request_id="req-123",
        params={"sender_id": "alice", "body": "hello"},
        target=NamespacedId("h", "/s", "tag", "s1", "t1"),
    )
    req_dict = req.to_dict()
    req_restored = RpcRequest.from_dict(req_dict)
    assert req_restored.op == "send"
    assert req_restored.request_id == "req-123"
    assert req_restored.params["body"] == "hello"
    assert req_restored.target == req.target

    resp = RpcResponse(
        request_id="req-123",
        ok=True,
        result={"message_id": "msg-999"},
    )
    resp_dict = resp.to_dict()
    resp_restored = RpcResponse.from_dict(resp_dict)
    assert resp_restored.ok is True
    assert resp_restored.result["message_id"] == "msg-999"


def make_fake_ssh_runner(store_dir: Path, cli_path: Path):
    """
    Creates a fake SSH runner that routes remote RPC commands
    to a local FileBus test store using the actual bus_cli.py.
    """
    def runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        assert cmd[0] == "ssh"
        assert "--" in cmd
        host_idx = cmd.index("--") + 1
        assert not cmd[host_idx].startswith("-")
        assert "python3" in cmd
        assert "--store" in cmd
        assert "rpc" in cmd

        # Route locally directly to bus_cli.py rpc
        exec_cmd = [sys.executable, str(cli_path), "--store", str(store_dir), "rpc"]
        proc = subprocess.Popen(
            exec_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = proc.communicate(input=stdin_data, timeout=timeout)
        return proc.returncode, stdout, stderr

    return runner


def test_full_lifecycle_over_rpc(tmp_path: Path):
    """Test full FileBus lifecycle (enroll, send, inbox, ack, reply) over typed RPC."""
    store_dir = tmp_path / "bus_store"
    store_dir.mkdir(parents=True, exist_ok=True)
    cli_path = Path(__file__).resolve().parents[1] / "coordination" / "bus_cli.py"

    client = SshFileBusClient(
        host="mock-remote-host",
        store_path=str(store_dir),
        bus_cli_path=str(cli_path),
        runner=make_fake_ssh_runner(store_dir, cli_path),
    )

    # 1. Enroll Alice
    id_a, tok_a = client.enroll(
        agent_name="alice",
        device_id="dev-hetzner",
        project_id="proj-distributed",
        task_id="task-alice-01",
    )
    assert id_a and tok_a

    # 2. Enroll Bob
    id_b, tok_b = client.enroll(
        agent_name="bob",
        device_id="dev-desktop",
        project_id="proj-distributed",
        task_id="task-bob-01",
    )
    assert id_b and tok_b

    # 3. Alice sends message to Bob
    idem_key = new_idempotency_key("test")
    msg_id = client.send(
        sender_id=id_a,
        token=tok_a,
        recipient_id=id_b,
        body="Hello from Alice over SSH FileBus RPC",
        data={"action": "reconcile", "attempt": 1},
        idempotency_key=idem_key,
    )
    assert msg_id

    # 4. Bob reads inbox
    inbox_b = client.inbox(identity_id=id_b, token=tok_b, unread_only=True)
    assert len(inbox_b) == 1
    received_msg = inbox_b[0]
    assert received_msg["message_id"] == msg_id
    assert received_msg["sender_id"] == id_a
    assert received_msg["body"] == "Hello from Alice over SSH FileBus RPC"
    assert received_msg["data"] == {"action": "reconcile", "attempt": 1}

    # 5. Bob acks message
    ack_res = client.ack(identity_id=id_b, token=tok_b, message_id=msg_id)
    assert ack_res["message_id"] == msg_id
    assert ack_res["acked_at"] is not None

    # Inbox unread should now be empty
    inbox_b_after = client.inbox(identity_id=id_b, token=tok_b, unread_only=True)
    assert len(inbox_b_after) == 0

    # 6. Bob replies to Alice
    reply_idem = new_idempotency_key("reply")
    reply_msg_id = client.reply(
        sender_id=id_b,
        token=tok_b,
        message_id=msg_id,
        body="Acknowledged by Bob over SSH RPC",
        data={"progress": "completed"},
        idempotency_key=reply_idem,
    )
    assert reply_msg_id

    # 7. Alice checks inbox and sees reply
    inbox_a = client.inbox(identity_id=id_a, token=tok_a, unread_only=True)
    assert len(inbox_a) == 1
    reply_msg = inbox_a[0]
    assert reply_msg["message_id"] == reply_msg_id
    assert reply_msg["sender_id"] == id_b
    assert reply_msg["reply_to"] == msg_id
    assert reply_msg["body"] == "Acknowledged by Bob over SSH RPC"


def test_typed_replay_and_idempotency_keys(tmp_path: Path):
    """Test idempotency key deduplication and conflict detection over RPC."""
    store_dir = tmp_path / "bus_store"
    store_dir.mkdir(parents=True, exist_ok=True)
    cli_path = Path(__file__).resolve().parents[1] / "coordination" / "bus_cli.py"

    client = SshFileBusClient(
        host="mock-remote-host",
        store_path=str(store_dir),
        bus_cli_path=str(cli_path),
        runner=make_fake_ssh_runner(store_dir, cli_path),
    )

    id_a, tok_a = client.enroll(agent_name="agent-a", device_id="dev-a")
    id_b, tok_b = client.enroll(agent_name="agent-b", device_id="dev-b")

    key = "tx-unique-idempotency-key-01"
    # First send
    msg1 = client.send(
        sender_id=id_a,
        token=tok_a,
        recipient_id=id_b,
        body="Payload 1",
        idempotency_key=key,
    )

    # Identical replay returns same message_id
    msg2 = client.send(
        sender_id=id_a,
        token=tok_a,
        recipient_id=id_b,
        body="Payload 1",
        idempotency_key=key,
    )
    assert msg1 == msg2

    # Conflicting replay under same key raises IdempotencyConflict
    with pytest.raises(IdempotencyConflict):
        client.send(
            sender_id=id_a,
            token=tok_a,
            recipient_id=id_b,
            body="Conflicting Changed Payload",
            idempotency_key=key,
        )


def test_negative_ssh_connection_failure():
    """Negative test: SSH exit 255 raises TransportError with fail-closed stderr omission (C2166)."""
    def fail_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        return 255, "", "ssh: connect to host unreachable-host port 22: Connection refused\n"

    client = SshFileBusClient(
        host="unreachable-host",
        store_path="/dummy/store",
        runner=fail_runner,
    )

    with pytest.raises(TransportError) as exc_info:
        client.enroll("agent", "dev")
    assert exc_info.value.exit_code == 255
    # Per C2166: Raw stderr is completely omitted from exception message and attributes
    assert "Connection refused" not in str(exc_info.value)
    assert not hasattr(exc_info.value, "stderr")
    assert exc_info.value.__cause__ is None


def test_negative_ssh_timeout():
    """Negative test: SSH timeout raises TransportTimeout with decoupled chaining (C2166)."""
    def timeout_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)

    client = SshFileBusClient(
        host="hanging-host",
        store_path="/dummy/store",
        timeout_sec=0.5,
        runner=timeout_runner,
    )

    with pytest.raises(TransportTimeout) as exc_info:
        client.inbox("ident", "tok")
    assert exc_info.value.timeout_sec == 0.5
    assert "timed out" in str(exc_info.value)
    # Per C2166: Decoupled exception chaining prevents traceback leak
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_negative_corrupted_banner_framing():
    """Negative test: Corrupted banner / non-JSON output raises FramingError with decoupled chaining (C2166)."""
    def noisy_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        corrupted_stdout = (
            "=====================================================\n"
            "Welcome to Ubuntu 24.04 LTS (GNU/Linux 6.8.0-generic)\n"
            "System load: 0.12, Memory usage: 42%\n"
            "=====================================================\n"
            "bash: /some/tool: command not found\n"
        )
        return 0, corrupted_stdout, ""

    client = SshFileBusClient(
        host="noisy-host",
        store_path="/dummy/store",
        runner=noisy_runner,
    )

    with pytest.raises(FramingError) as exc_info:
        client.ack("ident", "tok", "msg-1")
    assert "Failed to parse valid RPC response framing" in str(exc_info.value)
    assert exc_info.value.reason == "invalid_json_framing"
    # Per C2166: Raw output completely omitted from exception message and attributes
    assert "Ubuntu" not in str(exc_info.value)
    assert not hasattr(exc_info.value, "raw_output")
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_negative_auth_failure(tmp_path: Path):
    """Negative test: Invalid token or unauthorized operations raise AuthError (C2166)."""
    store_dir = tmp_path / "bus_store"
    store_dir.mkdir(parents=True, exist_ok=True)
    cli_path = Path(__file__).resolve().parents[1] / "coordination" / "bus_cli.py"

    client = SshFileBusClient(
        host="mock-remote-host",
        store_path=str(store_dir),
        bus_cli_path=str(cli_path),
        runner=make_fake_ssh_runner(store_dir, cli_path),
    )

    id_a, _ = client.enroll(agent_name="agent-a", device_id="dev-a")
    id_b, _ = client.enroll(agent_name="agent-b", device_id="dev-b")

    # Invalid token raises AuthError
    with pytest.raises(AuthError) as exc_info:
        client.send(
            sender_id=id_a,
            token="invalid-bogus-token",
            recipient_id=id_b,
            body="Unauthorized message",
        )
    assert "auth_failed" in str(exc_info.value)
    assert exc_info.value.reason == "remote_auth_rejected"


def test_zero_credentials_in_command_argv(tmp_path: Path):
    """Verify that neither tokens, bodies, nor secrets ever appear in process argv."""
    captured_commands: list[list[str]] = []
    store_dir = tmp_path / "bus_store"
    store_dir.mkdir(parents=True, exist_ok=True)
    cli_path = Path(__file__).resolve().parents[1] / "coordination" / "bus_cli.py"
    real_runner = make_fake_ssh_runner(store_dir, cli_path)

    def spy_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        captured_commands.append(list(cmd))
        return real_runner(cmd, stdin_data, timeout)

    client = SshFileBusClient(
        host="audit-host",
        store_path=str(store_dir),
        bus_cli_path=str(cli_path),
        runner=spy_runner,
    )

    id_a, tok_a = client.enroll(agent_name="agent-a", device_id="dev-a")
    secret_body = "HIGHLY_CONFIDENTIAL_PAYLOAD_BODY_777"
    client.send(
        sender_id=id_a,
        token=tok_a,
        recipient_id=id_a,
        body=secret_body,
    )

    assert len(captured_commands) >= 2
    for cmd in captured_commands:
        cmd_str = " ".join(cmd)
        assert tok_a not in cmd_str, f"Token leaked in argv: {cmd_str}"
        assert secret_body not in cmd_str, f"Secret body leaked in argv: {cmd_str}"


# ---------------------------------------------------------------------------
# C2162 & C2164 Hardening Tests
# ---------------------------------------------------------------------------


def test_c2162_host_option_injection_rejected():
    """Verify that host starting with '-' is rejected and '--' precedes host in argv."""
    # Rejects host starting with '-'
    with pytest.raises(ValueError, match="Host parameter must not start with '-'"):
        SshFileBusClient(host="-oProxyCommand=touch /tmp/pwned", store_path="/store")

    with pytest.raises(ValueError, match="Host parameter must not start with '-'"):
        SshFileBusClient(host="-v", store_path="/store")

    # Valid host puts '--' before host in command
    captured_cmd: list[str] = []

    def capture_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        captured_cmd.extend(cmd)
        return 0, json.dumps({"request_id": json.loads(stdin_data)["request_id"], "ok": True, "result": {"identity_id": "i1", "token": "t1"}}), ""

    client = SshFileBusClient(host="remote.example.com", store_path="/store", runner=capture_runner)
    client.enroll("a1", "d1")

    assert "--" in captured_cmd
    double_dash_idx = captured_cmd.index("--")
    assert captured_cmd[double_dash_idx + 1] == "remote.example.com"


def test_c2162_remote_shell_escaping():
    """Verify that remote command arguments are quoted with shlex.quote for POSIX login shells."""
    captured_cmd: list[str] = []

    def capture_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        captured_cmd.extend(cmd)
        return 0, json.dumps({"request_id": json.loads(stdin_data)["request_id"], "ok": True, "result": {"identity_id": "i1", "token": "t1"}}), ""

    tricky_store = "/data/my store with spaces/bus;rm -rf /"
    tricky_cli = "/opt/tools/bus cli.py"
    client = SshFileBusClient(
        host="remote-box",
        store_path=tricky_store,
        bus_cli_path=tricky_cli,
        runner=capture_runner,
    )
    client.enroll("a1", "d1")

    # Command arguments must contain shlex quoted versions
    assert shlex.quote(tricky_store) in captured_cmd
    assert shlex.quote(tricky_cli) in captured_cmd


def test_c2162_strict_correlation_check():
    """Verify that request_id mismatch raises FramingError('request_id_mismatch') from None (C2166)."""
    def mismatch_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        # Return valid JSON but with wrong request_id
        return 0, json.dumps({"request_id": "completely-wrong-id", "ok": True, "result": {}}), ""

    client = SshFileBusClient(host="mock-host", store_path="/store", runner=mismatch_runner)
    with pytest.raises(FramingError, match="request_id_mismatch") as exc_info:
        client.enroll("a1", "d1")
    assert exc_info.value.reason == "request_id_mismatch"
    assert exc_info.value.__cause__ is None
    # Payload must NOT be printed in the exception message
    assert "completely-wrong-id" not in str(exc_info.value)


def test_c2162_strict_boolean_check():
    """Verify that non-boolean 'ok' values raise FramingError."""
    def string_bool_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        # Return string "true" instead of bool True
        req_id = json.loads(stdin_data)["request_id"]
        return 0, json.dumps({"request_id": req_id, "ok": "true", "result": {}}), ""

    client = SshFileBusClient(host="mock-host", store_path="/store", runner=string_bool_runner)
    with pytest.raises(FramingError, match="strict boolean"):
        client.enroll("a1", "d1")


def test_c2162_strict_exit_code_check():
    """Verify that any non-zero exit code fails closed with TransportError even if JSON is present."""
    def nonzero_with_json_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        req_id = json.loads(stdin_data)["request_id"]
        # Process output valid JSON but crashed / exited 1
        return 1, json.dumps({"request_id": req_id, "ok": True, "result": {"identity_id": "i1"}}), "Segmentation fault\n"

    client = SshFileBusClient(host="mock-host", store_path="/store", runner=nonzero_with_json_runner)
    with pytest.raises(TransportError) as exc_info:
        client.enroll("a1", "d1")
    assert exc_info.value.exit_code == 1
    assert "failed with exit 1" in str(exc_info.value)
    # Per C2166: stderr is omitted
    assert "Segmentation fault" not in str(exc_info.value)
    assert not hasattr(exc_info.value, "stderr")


def test_c2162_error_redaction_helper():
    """Verify utility helper _redact sanitizes tokens and private payloads."""
    raw = 'Error occurred: {"token": "secret_token_12345", "body": "private_message_text"}'
    redacted = _redact(raw)
    assert "secret_token_12345" not in redacted
    assert "private_message_text" not in redacted
    assert "[REDACTED]" in redacted

    raw_cli = "ssh failed: token=super_secret_tok123 Authorization: Bearer abcdef12345"
    redacted_cli = _redact(raw_cli)
    assert "super_secret_tok123" not in redacted_cli
    assert "abcdef12345" not in redacted_cli


# ---------------------------------------------------------------------------
# C2166 Specific Tests
# ---------------------------------------------------------------------------


def test_c2166_mandatory_ssh_options():
    """Verify that SshFileBusClient enforces -o BatchMode=yes and -o StrictHostKeyChecking=yes, rejecting insecure options."""
    captured_cmd: list[str] = []

    def mock_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        captured_cmd.extend(cmd)
        req = json.loads(stdin_data or "{}")
        return 0, json.dumps({"request_id": req["request_id"], "ok": True, "result": {}}), ""

    # Default client includes mandatory options
    client = SshFileBusClient(host="secure.host", store_path="/store", runner=mock_runner)
    client.ack("id1", "tok1", "m1")

    cmd_str = " ".join(captured_cmd)
    assert "-o BatchMode=yes" in cmd_str
    assert "-o StrictHostKeyChecking=yes" in cmd_str

    # Prohibit ambient accept-new or no
    with pytest.raises(ValueError, match="StrictHostKeyChecking=accept-new is prohibited"):
        SshFileBusClient(host="host", store_path="/s", ssh_opts=["-o", "StrictHostKeyChecking=accept-new"])

    with pytest.raises(ValueError, match="StrictHostKeyChecking=no is prohibited"):
        SshFileBusClient(host="host", store_path="/s", ssh_opts=["-o", "StrictHostKeyChecking=no"])


def test_c2166_fail_closed_omission_no_stdout_stderr_leaks():
    """
    Verify that raw stdout and stderr are completely omitted from public exception
    messages and attributes, preventing any leakage of credentials or message bodies.
    """
    secret_token = "secret-token-abcdef-99999"
    secret_body = "SUPER_SECRET_PAYLOAD_BODY_CONFIDENTIAL"

    # Case 1: TransportError from non-zero exit with sensitive stderr
    def runner_leak_stderr(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        noisy_stderr = f"Fatal crash in worker: token={secret_token}, body={secret_body}\n"
        return 2, "", noisy_stderr

    client1 = SshFileBusClient(host="remote.box", store_path="/store", runner=runner_leak_stderr)
    with pytest.raises(TransportError) as exc_info:
        client1.send(sender_id="s1", token=secret_token, recipient_id="r1", body=secret_body)

    err_str = str(exc_info.value)
    assert secret_token not in err_str
    assert secret_body not in err_str
    assert not hasattr(exc_info.value, "stderr")
    assert exc_info.value.exit_code == 2

    # Case 2: FramingError from malformed stdout echoing sensitive payload
    def runner_leak_stdout(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        malformed_stdout = f"Echoing request before crash: token={secret_token} body={secret_body}"
        return 0, malformed_stdout, ""

    client2 = SshFileBusClient(host="remote.box", store_path="/store", runner=runner_leak_stdout)
    with pytest.raises(FramingError) as exc_info2:
        client2.send(sender_id="s1", token=secret_token, recipient_id="r1", body=secret_body)

    framing_err_str = str(exc_info2.value)
    assert secret_token not in framing_err_str
    assert secret_body not in framing_err_str
    assert not hasattr(exc_info2.value, "raw_output")


def test_c2166_exception_chaining_decoupled():
    """Verify that TimeoutExpired and JSONDecodeError do not leak unredacted data via __cause__ or __context__."""
    # Case 1: TimeoutExpired
    def timeout_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        exc = subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)
        exc.stdout = "sensitive stdout in timeout exception"
        exc.stderr = "sensitive stderr in timeout exception"
        raise exc

    client_timeout = SshFileBusClient(host="remote.box", store_path="/store", timeout_sec=0.1, runner=timeout_runner)
    with pytest.raises(TransportTimeout) as exc_info:
        client_timeout.inbox("id1", "tok1")
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None

    # Case 2: JSONDecodeError
    def json_err_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        return 0, "Non-JSON with secret: token=super_secret_tok123", ""

    client_json = SshFileBusClient(host="remote.box", store_path="/store", runner=json_err_runner)
    with pytest.raises(FramingError) as exc_info2:
        client_json.inbox("id1", "tok1")
    assert exc_info2.value.__cause__ is None
    assert exc_info2.value.__context__ is None
