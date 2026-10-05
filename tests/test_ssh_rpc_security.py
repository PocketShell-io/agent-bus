"""Adversarial Security Test Suite for Typed SSH FileBus RPC.

Directives:
- Codex Principal C2162 / C2164 / C2166.
- Strict option injection defense (rejects host starting with '-', passes '--' before host).
- Remote shell metacharacter and argument escaping (shlex.quote for POSIX shells, Windows held).
- Strict correlation and boolean coercion (string 'false' fails closed, request_id mismatch raises FramingError).
- Nonzero returncode fails closed with TransportError.
- Strict known_hosts: -o BatchMode=yes and -o StrictHostKeyChecking=yes present in default SSH arguments.
- Timeout ambiguity: TransportTimeout preserves ambiguous side-effect (UNKNOWN outcome) with zero auto-retries.
- Leakage verification across all error paths:
  * Secret tokens (tok_fake_secret_987654321) and payloads (sensitive_user_body_12345) never leak in str(exc)
  * Public attributes (code, details, stderr, raw_output) never leak secret material
  * exc.__cause__ is None (verifying raise ... from None prevents traceback leakage)
"""

from __future__ import annotations

import json
from pathlib import Path
import re
import shlex
import subprocess
import pytest

from coordination.envelope import (
    NamespacedId,
    RpcRequest,
    RpcResponse,
    new_request_id,
)
from coordination.errors import (
    AuthError,
    CoordinationError,
    FramingError,
    IdempotencyConflict,
    TransportError,
    TransportTimeout,
)
from coordination.ssh_rpc import SshFileBusClient, _redact, _validate_host


FAKE_SECRET_TOKEN = "tok_fake_secret_987654321"
SENSITIVE_USER_BODY = "sensitive_user_body_12345"


# =============================================================================
# 1. Local SSH Option Injection Defense & Known Hosts
# =============================================================================

def test_ssh_default_opts_strict_host_key_checking():
    """Verify that default SSH options enforce BatchMode=yes and StrictHostKeyChecking=yes."""
    client = SshFileBusClient(host="safe-host.internal", store_path="/path/to/store")
    assert "-o" in client.ssh_opts
    assert "BatchMode=yes" in client.ssh_opts
    assert "StrictHostKeyChecking=yes" in client.ssh_opts


def test_ssh_option_normalization_case_and_spacing():
    """Verify OpenSSH options normalize case-insensitively with varied spacing and tokenization."""
    # Case variation & whitespace around '='
    client1 = SshFileBusClient(
        host="host1",
        store_path="/store",
        ssh_opts=["-o", "batchmode = yes", "-o", "stricthostkeychecking = yes"],
    )
    assert "-o" in client1.ssh_opts
    assert "BatchMode=yes" in client1.ssh_opts
    assert "StrictHostKeyChecking=yes" in client1.ssh_opts

    # Attached form (-oKey=Value)
    client2 = SshFileBusClient(
        host="host2",
        store_path="/store",
        ssh_opts=["-obatchmode=yes", "-ostricthostkeychecking=yes"],
    )
    assert "BatchMode=yes" in client2.ssh_opts
    assert "StrictHostKeyChecking=yes" in client2.ssh_opts

    # Space-separated keyword and value (-o, Key, Val)
    client3 = SshFileBusClient(
        host="host3",
        store_path="/store",
        ssh_opts=["-o", "BatchMode", "yes", "-o", "StrictHostKeyChecking", "yes"],
    )
    assert "BatchMode=yes" in client3.ssh_opts
    assert "StrictHostKeyChecking=yes" in client3.ssh_opts


def test_ssh_option_rejection_case_and_spacing():
    """Verify that any non-'yes' value for StrictHostKeyChecking or BatchMode fails closed across all formats."""
    bypass_attempts = [
        ["-o", "StrictHostKeyChecking=accept-new"],
        ["-o", "stricthostkeychecking = accept-new"],
        ["-o", "StrictHostKeyChecking accept-new"],
        ["-o", "stricthostkeychecking=no"],
        ["-o", "STRICTHOSTKEYCHECKING=no"],
        ["-o", "StrictHostKeyChecking = no"],
        ["-oStrictHostKeyChecking=no"],
        ["-o", "StrictHostKeyChecking=off"],
        ["-o", "StrictHostKeyChecking=ask"],
        ["-o", "BatchMode=no"],
        ["-o", "batchmode = no"],
        ["-oBatchMode=no"],
        ["-o", "BatchMode no"],
        ["-o", "BATCHMODE=off"],
    ]
    for bad_opts in bypass_attempts:
        with pytest.raises(ValueError, match="(StrictHostKeyChecking|BatchMode) must be 'yes'"):
            SshFileBusClient(host="host", store_path="/store", ssh_opts=bad_opts)


def test_ssh_duplicate_options_rejection_and_precedence():
    """Verify that duplicate conflicting options fail closed regardless of order."""
    conflicting_pairs = [
        ["-o", "StrictHostKeyChecking=yes", "-o", "StrictHostKeyChecking=no"],
        ["-o", "StrictHostKeyChecking=no", "-o", "StrictHostKeyChecking=yes"],
        ["-o", "BatchMode=yes", "-o", "batchmode=no"],
        ["-o", "batchmode=no", "-o", "BatchMode=yes"],
    ]
    for bad_pair in conflicting_pairs:
        with pytest.raises(ValueError, match="(StrictHostKeyChecking|BatchMode) must be 'yes'"):
            SshFileBusClient(host="host", store_path="/store", ssh_opts=bad_pair)

    # Duplicate identical valid options normalize cleanly
    client_dup = SshFileBusClient(
        host="host",
        store_path="/store",
        ssh_opts=["-o", "BatchMode=yes", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes"],
    )
    assert client_dup.ssh_opts.count("BatchMode=yes") == 1
    assert client_dup.ssh_opts.count("StrictHostKeyChecking=yes") == 1


def test_ssh_proxycommand_custom_trust():
    """Verify that ProxyCommand, LocalCommand, and PermitLocalCommand are rejected without custom trust."""
    untrusted_directives = [
        ["-o", "ProxyCommand=touch /tmp/pwned"],
        ["-o", "proxycommand=nc %h %p"],
        ["-oProxyCommand=whoami"],
        ["-o", "LocalCommand=id"],
        ["-o", "PermitLocalCommand=yes"],
    ]
    for untrusted in untrusted_directives:
        with pytest.raises(ValueError, match="(is untrusted and prohibited|is prohibited; only vetted options)"):
            SshFileBusClient(host="host", store_path="/store", ssh_opts=untrusted)

    # When explicit custom trust is granted, ProxyCommand is permitted
    trusted_proxy = ["-o", "ProxyCommand=ssh -W %h:%p jumpbox.internal"]
    client_trusted = SshFileBusClient(
        host="host",
        store_path="/store",
        ssh_opts=trusted_proxy,
        allow_custom_proxycommand=True,
    )
    assert any("proxycommand=ssh -W %h:%p jumpbox.internal" in opt for opt in client_trusted.ssh_opts)


def test_ssh_c2181_bare_positional_arguments_rejected():
    """Verify that bare positional arguments in ssh_opts fail closed to prevent destination hijacking."""
    prohibited_positionals = [
        ["attacker.com"],
        ["evil.host", "-o", "BatchMode=yes"],
        ["-o", "BatchMode=yes", "rogue-host"],
    ]
    for bad_opts in prohibited_positionals:
        with pytest.raises(ValueError, match="Positional destination argument in ssh_opts"):
            SshFileBusClient(host="intended.host", store_path="/store", ssh_opts=bad_opts)


def test_ssh_c2181_config_file_and_flags_allowlist():
    """Verify narrow flag allowlist and -F custom config defense under C2181."""
    # -F custom config rejected by default
    with pytest.raises(ValueError, match="OpenSSH config file option '-F' is untrusted and prohibited"):
        SshFileBusClient(host="host", store_path="/store", ssh_opts=["-F", "/tmp/bad_config"])

    with pytest.raises(ValueError, match="OpenSSH config file option '-F' is untrusted and prohibited"):
        SshFileBusClient(host="host", store_path="/store", ssh_opts=["-F/tmp/bad_config"])

    # -F allowed with explicit custom trust
    client_cfg = SshFileBusClient(
        host="host",
        store_path="/store",
        ssh_opts=["-F", "/path/to/custom_config"],
        allow_custom_proxycommand=True,
    )
    assert "-F" in client_cfg.ssh_opts
    assert "/path/to/custom_config" in client_cfg.ssh_opts

    # Valid allowed flags: -p (port), -i (identity), -C (compression), -q (quiet)
    client_allowed = SshFileBusClient(
        host="host",
        store_path="/store",
        ssh_opts=["-p", "2222", "-C", "-q"],
    )
    assert "-p" in client_allowed.ssh_opts
    assert "2222" in client_allowed.ssh_opts
    assert "-C" in client_allowed.ssh_opts

    # Invalid port rejected
    with pytest.raises(ValueError, match="requires a valid integer port"):
        SshFileBusClient(host="host", store_path="/store", ssh_opts=["-p", "999999"])

    # Disallowed flag rejected
    with pytest.raises(ValueError, match="Prohibited or unrecognized SSH option flag"):
        SshFileBusClient(host="host", store_path="/store", ssh_opts=["-D", "1080"])


def test_ssh_c2185_option_key_allowlist_and_default_deny():
    """Verify default-deny allowlist rejects arbitrary executable/inclusion directives (C2185/C2186)."""
    prohibited_options = [
        ["-o", "KnownHostsCommand=/tmp/evil_lookup"],
        ["-oKnownHostsCommand=whoami"],
        ["-o", "knownhostscommand = /tmp/lookup"],
        ["-o", "Include=/etc/ssh/malicious_include"],
        ["-oInclude /tmp/cfg"],
        ["-o", "LocalCommand=id"],
        ["-o", "PermitLocalCommand=yes"],
        ["-o", "PKCS11Provider=/tmp/libevil.so"],
        ["-o", "Match=all"],
        ["-o", "UserKnownHostsFile=/dev/null"],
        ["-o", "ArbitraryOption=evil"],
    ]
    for bad_opt in prohibited_options:
        with pytest.raises(ValueError, match="(prohibited|only vetted options)"):
            SshFileBusClient(host="host", store_path="/store", ssh_opts=bad_opt)

    # Allowed safe options pass
    client_safe = SshFileBusClient(
        host="host",
        store_path="/store",
        ssh_opts=["-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15", "-o", "Compression=yes"],
    )
    assert any("connecttimeout=10" in opt for opt in client_safe.ssh_opts)
    assert any("serveraliveinterval=15" in opt for opt in client_safe.ssh_opts)


def test_ssh_option_injection_host_rejected_at_init():
    """Verify SshFileBusClient rejects hostnames starting with '-' before process spawn."""
    malicious_hosts = [
        "-oProxyCommand=touch /tmp/pwned",
        "-F/tmp/bad_config",
        "-i/tmp/id_rsa",
        " -oProxyCommand=whoami",
        "---badhost",
    ]
    for bad_host in malicious_hosts:
        with pytest.raises(ValueError, match="must not start with '-'"):
            SshFileBusClient(host=bad_host, store_path="/path/to/store")

    with pytest.raises(ValueError, match="must be non-empty"):
        SshFileBusClient(host="", store_path="/path/to/store")
    with pytest.raises(ValueError, match="must be non-empty"):
        SshFileBusClient(host="   ", store_path="/path/to/store")


def test_ssh_command_argv_includes_double_dash_delimiter():
    """Verify that SshFileBusClient inserts '--' before host in ssh argv."""
    captured_cmds: list[list[str]] = []

    def mock_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        captured_cmds.append(cmd)
        req = json.loads(stdin_data or "{}")
        resp = RpcResponse(request_id=req.get("request_id", ""), ok=True, result={"ok": True})
        return 0, json.dumps(resp.to_dict()), ""

    client = SshFileBusClient(
        host="safe-host.example.com",
        store_path="/path/to/store",
        runner=mock_runner,
    )
    client.inbox(identity_id="ident-1", token="tok-1")

    assert len(captured_cmds) == 1
    cmd = captured_cmds[0]
    assert cmd[0] == "ssh"
    assert "--" in cmd
    dash_dash_idx = cmd.index("--")
    assert cmd[dash_dash_idx + 1] == "safe-host.example.com"


# =============================================================================
# 2. Remote Shell Metacharacter & Argument Escaping (POSIX)
# =============================================================================

def test_remote_shell_argument_escaping_posix():
    """
    Verify that bus_cli_path and store_path containing shell metacharacters
    (semicolons, subshells, spaces, quotes) are safely quoted with shlex.quote()
    for POSIX remote login shells ($SHELL/bash/sh).
    Note (C2164): Windows remote shells (cmd.exe/PowerShell) are HELD pending Windows tests.
    """
    captured_cmds: list[list[str]] = []

    def mock_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        captured_cmds.append(cmd)
        req = json.loads(stdin_data or "{}")
        resp = RpcResponse(request_id=req.get("request_id", ""), ok=True, result={"identity_id": "i1", "token": "t1"})
        return 0, json.dumps(resp.to_dict()), ""

    bad_store_path = "/tmp/store; touch /tmp/injected && rm -rf /"
    bad_cli_path = "coordination/bus cli.py$(id)`whoami`"

    client = SshFileBusClient(
        host="remote-host",
        store_path=bad_store_path,
        bus_cli_path=bad_cli_path,
        runner=mock_runner,
    )
    client.enroll(agent_name="agent-a", device_id="dev-1")

    assert len(captured_cmds) == 1
    cmd = captured_cmds[0]

    expected_quoted_store = shlex.quote(bad_store_path)
    expected_quoted_cli = shlex.quote(bad_cli_path)

    assert expected_quoted_cli in cmd
    assert expected_quoted_store in cmd

    remote_command_str = " ".join(cmd[cmd.index("remote-host") + 1:])
    parsed_tokens = shlex.split(remote_command_str)
    assert parsed_tokens[0] == "python3"
    assert parsed_tokens[1] == bad_cli_path
    assert parsed_tokens[2] == "--store"
    assert parsed_tokens[3] == bad_store_path
    assert parsed_tokens[4] == "rpc"


# =============================================================================
# 3. Strict Correlation & Protocol Failures
# =============================================================================

def test_strict_correlation_request_id_mismatch_raises_framing_error():
    """Verify that a remote response returning a different request_id raises FramingError."""
    def mock_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        bogus_resp = {"request_id": "spoofed-request-id-999", "ok": True, "result": {"data": 123}}
        return 0, json.dumps(bogus_resp), ""

    client = SshFileBusClient(
        host="remote-host",
        store_path="/store",
        runner=mock_runner,
    )
    with pytest.raises(FramingError) as exc_info:
        client.inbox(identity_id="id1", token="tok1")
    assert exc_info.value.reason == "request_id_mismatch"


def test_boolean_coercion_fails_closed():
    """
    Verify that string "false", "0", or non-boolean truthy values for 'ok'
    strictly fail closed and are NOT coerced to True.
    """
    def runner_str_false(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        req = json.loads(stdin_data or "{}")
        resp = {"request_id": req["request_id"], "ok": "false", "result": {"pwn": True}}
        return 0, json.dumps(resp), ""

    client1 = SshFileBusClient(host="remote-host", store_path="/store", runner=runner_str_false)
    with pytest.raises(FramingError) as exc_info1:
        client1.send(sender_id="s1", token="t1", recipient_id="r1", body="hello")
    assert exc_info1.value.code == "framing_error"
    assert exc_info1.value.reason in ("schema_validation_failed", "invalid_bool_type")

    def runner_str_zero(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        req = json.loads(stdin_data or "{}")
        resp = {"request_id": req["request_id"], "ok": "0", "result": {}}
        return 0, json.dumps(resp), ""

    client2 = SshFileBusClient(host="remote-host", store_path="/store", runner=runner_str_zero)
    with pytest.raises(FramingError) as exc_info2:
        client2.inbox(identity_id="i1", token="t1")
    assert exc_info2.value.code == "framing_error"
    assert exc_info2.value.reason in ("schema_validation_failed", "invalid_bool_type")

    def runner_int_one(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        req = json.loads(stdin_data or "{}")
        resp = {"request_id": req["request_id"], "ok": 1, "result": {}}
        return 0, json.dumps(resp), ""

    client3 = SshFileBusClient(host="remote-host", store_path="/store", runner=runner_int_one)
    with pytest.raises(FramingError) as exc_info3:
        client3.ack(identity_id="i1", token="t1", message_id="m1")
    assert exc_info3.value.code == "framing_error"
    assert exc_info3.value.reason in ("schema_validation_failed", "invalid_bool_type")


def test_non_zero_exit_code_fails_closed_despite_valid_stdout_json():
    """
    Verify that if the remote command exits with non-zero (e.g. exit 1 or 127),
    the client fails closed with TransportError even if stdout contains valid JSON.
    """
    def mock_runner_exit_1(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        req = json.loads(stdin_data or "{}")
        valid_json = json.dumps({"request_id": req["request_id"], "ok": True, "result": "should_be_ignored"})
        return 1, valid_json, "Process failed unexpectedly: exit 1"

    client = SshFileBusClient(host="remote-host", store_path="/store", runner=mock_runner_exit_1)
    with pytest.raises(TransportError, match="SSH command failed with exit 1") as exc_info:
        client.reply(sender_id="s1", token="t1", message_id="m1", body="rep")
    assert exc_info.value.exit_code == 1


def test_preamble_banner_noise_fails_closed():
    """
    Verify that dirty stdout framing (preamble noise, multi-object banners)
    strictly fails closed with FramingError and does not accept rogue JSON lines.
    """
    def runner_banner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        req = json.loads(stdin_data or "{}")
        noisy_output = (
            "System is booting...\n"
            '{"banner": "motd", "ok": true, "request_id": "bogus"}\n'
            f'{{"request_id": "{req["request_id"]}", "ok": true, "result": {{}}}}\n'
        )
        return 0, noisy_output, ""

    client = SshFileBusClient(host="remote-host", store_path="/store", runner=runner_banner)
    with pytest.raises(FramingError, match="Failed to parse valid RPC response framing"):
        client.inbox(identity_id="i1", token="t1")


# =============================================================================
# 4. Timeout Semantics: Ambiguous Outcome Preservation
# =============================================================================

def test_transport_timeout_preserves_ambiguous_outcome_without_auto_retry():
    """
    Verify that TransportTimeout raises on subprocess timeout, preserving
    ambiguous UNKNOWN side-effects with zero automatic mutating retries.
    """
    call_count = 0

    def runner_timeout(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        nonlocal call_count
        call_count += 1
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)

    client = SshFileBusClient(host="remote-host", store_path="/store", timeout_sec=1.5, runner=runner_timeout)
    with pytest.raises(TransportTimeout, match="SSH RPC timed out after 1.5s"):
        client.send(sender_id="s1", token=FAKE_SECRET_TOKEN, recipient_id="r1", body=SENSITIVE_USER_BODY)

    assert call_count == 1, "Client must NOT perform automatic mutating retries on timeout"


# =============================================================================
# 5. Codex C2166: Leakage Verification Across All Error Paths
# =============================================================================

def _assert_no_leakage_and_no_cause(exc: Exception):
    """
    Asserts that:
    1. str(exc) does not contain FAKE_SECRET_TOKEN or SENSITIVE_USER_BODY.
    2. All public attributes (code, details, stderr, raw_output, etc.) do not contain sensitive tokens.
    3. exc.__cause__ is None (verifying 'raise ... from None' suppresses traceback leakage).
    """
    # 1. Check str representation
    exc_str = str(exc)
    assert FAKE_SECRET_TOKEN not in exc_str, f"Token leaked in str(exc): {exc_str}"
    assert SENSITIVE_USER_BODY not in exc_str, f"Payload body leaked in str(exc): {exc_str}"

    # 2. Check public attributes
    for attr in dir(exc):
        if attr.startswith("_"):
            continue
        val = getattr(exc, attr)
        if callable(val):
            continue
        val_str = json.dumps(val) if isinstance(val, (dict, list)) else str(val)
        assert FAKE_SECRET_TOKEN not in val_str, f"Token leaked in exc.{attr}: {val_str}"
        assert SENSITIVE_USER_BODY not in val_str, f"Payload body leaked in exc.{attr}: {val_str}"

    # 3. Check cause suppression
    assert exc.__cause__ is None, f"exc.__cause__ must be None (found {type(exc.__cause__).__name__})"


def test_leakage_path_1_transport_failure_stderr():
    """Verify transport failure with leaking stderr redacts tokens and sets cause=None."""
    def runner_leak_stderr(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        noisy_stderr = (
            f'Remote error: token={FAKE_SECRET_TOKEN} failed with body="{SENSITIVE_USER_BODY}"\n'
            f'Details: {{"token": "{FAKE_SECRET_TOKEN}", "body": "{SENSITIVE_USER_BODY}"}}'
        )
        return 1, "", noisy_stderr

    client = SshFileBusClient(host="remote-host", store_path="/store", runner=runner_leak_stderr)
    with pytest.raises(TransportError) as exc_info:
        client.send(sender_id="s1", token=FAKE_SECRET_TOKEN, recipient_id="r1", body=SENSITIVE_USER_BODY)

    _assert_no_leakage_and_no_cause(exc_info.value)
    assert exc_info.value.code == "remote_transport_failed"
    assert exc_info.value.exit_code == 1


def test_leakage_path_2_subprocess_exception():
    """Verify unexpected subprocess exceptions redact tokens and set cause=None."""
    def runner_crash(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        raise OSError(f"Subprocess IO error with token={FAKE_SECRET_TOKEN} and data={SENSITIVE_USER_BODY}")

    client = SshFileBusClient(host="remote-host", store_path="/store", runner=runner_crash)
    with pytest.raises(TransportError) as exc_info:
        client.send(sender_id="s1", token=FAKE_SECRET_TOKEN, recipient_id="r1", body=SENSITIVE_USER_BODY)

    _assert_no_leakage_and_no_cause(exc_info.value)


def test_leakage_path_3_timeout_expired():
    """Verify TimeoutExpired preserves no cause and redacts token in traceback."""
    def runner_timeout(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        # TimeoutExpired can carry output and stderr
        raise subprocess.TimeoutExpired(
            cmd=cmd,
            timeout=timeout,
            output=f"stdout with {FAKE_SECRET_TOKEN}",
            stderr=f"stderr with {SENSITIVE_USER_BODY}",
        )

    client = SshFileBusClient(host="remote-host", store_path="/store", timeout_sec=2.0, runner=runner_timeout)
    with pytest.raises(TransportTimeout) as exc_info:
        client.send(sender_id="s1", token=FAKE_SECRET_TOKEN, recipient_id="r1", body=SENSITIVE_USER_BODY)

    _assert_no_leakage_and_no_cause(exc_info.value)


def test_leakage_path_4_invalid_framing_json_decode():
    """Verify JSONDecodeError does not leak unredacted raw doc via __cause__ or raw_output."""
    def runner_bad_json(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        # Malformed JSON echoing token and body
        corrupt_stdout = f'{{"echo_token": "{FAKE_SECRET_TOKEN}", "body": "{SENSITIVE_USER_BODY}", incomplete: true'
        return 0, corrupt_stdout, ""

    client = SshFileBusClient(host="remote-host", store_path="/store", runner=runner_bad_json)
    with pytest.raises(FramingError) as exc_info:
        client.send(sender_id="s1", token=FAKE_SECRET_TOKEN, recipient_id="r1", body=SENSITIVE_USER_BODY)

    _assert_no_leakage_and_no_cause(exc_info.value)
    assert exc_info.value.code == "framing_error"
    assert exc_info.value.reason == "invalid_json_framing"


def test_leakage_path_5_invalid_framing_non_dict_and_value_error():
    """Verify non-dict response or ValueError on from_dict suppresses cause and redacts."""
    # Case A: Non-dict
    def runner_array(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        return 0, json.dumps([FAKE_SECRET_TOKEN, SENSITIVE_USER_BODY]), ""

    client1 = SshFileBusClient(host="remote-host", store_path="/store", runner=runner_array)
    with pytest.raises(FramingError) as exc_info1:
        client1.send(sender_id="s1", token=FAKE_SECRET_TOKEN, recipient_id="r1", body=SENSITIVE_USER_BODY)
    _assert_no_leakage_and_no_cause(exc_info1.value)

    # Case B: ValueError in from_dict (e.g. missing request_id)
    def runner_missing_req(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        return 0, json.dumps({"echo": FAKE_SECRET_TOKEN, "payload": SENSITIVE_USER_BODY}), ""

    client2 = SshFileBusClient(host="remote-host", store_path="/store", runner=runner_missing_req)
    with pytest.raises(FramingError) as exc_info2:
        client2.send(sender_id="s1", token=FAKE_SECRET_TOKEN, recipient_id="r1", body=SENSITIVE_USER_BODY)
    _assert_no_leakage_and_no_cause(exc_info2.value)


def test_leakage_path_6_auth_failure_with_echoed_credentials():
    """Verify AuthError redacts tokens and bodies in details and message and has cause=None."""
    def runner_auth_fail(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        req = json.loads(stdin_data or "{}")
        err_resp = {
            "request_id": req.get("request_id"),
            "ok": False,
            "error": {
                "code": "auth_failed",
                "message": f"Invalid credentials supplied: token={FAKE_SECRET_TOKEN}",
                "token": FAKE_SECRET_TOKEN,
                "body": SENSITIVE_USER_BODY,
                "details": {
                    "token": FAKE_SECRET_TOKEN,
                    "body": SENSITIVE_USER_BODY,
                    "nested_leak": f"Bearer {FAKE_SECRET_TOKEN}",
                },
            },
        }
        return 0, json.dumps(err_resp), ""

    client = SshFileBusClient(host="remote-host", store_path="/store", runner=runner_auth_fail)
    with pytest.raises(AuthError) as exc_info:
        client.send(sender_id="s1", token=FAKE_SECRET_TOKEN, recipient_id="r1", body=SENSITIVE_USER_BODY)

    _assert_no_leakage_and_no_cause(exc_info.value)
    assert exc_info.value.code == "auth_error"
    assert exc_info.value.reason == "remote_auth_rejected"


def test_leakage_path_7_request_id_mismatch():
    """Verify request_id mismatch raises FramingError without cause or token leakage."""
    def runner_mismatch(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
        err_resp = {
            "request_id": "wrong-request-id",
            "ok": True,
            "result": {"echo_token": FAKE_SECRET_TOKEN, "echo_body": SENSITIVE_USER_BODY},
        }
        return 0, json.dumps(err_resp), ""

    client = SshFileBusClient(host="remote-host", store_path="/store", runner=runner_mismatch)
    with pytest.raises(FramingError) as exc_info:
        client.send(sender_id="s1", token=FAKE_SECRET_TOKEN, recipient_id="r1", body=SENSITIVE_USER_BODY)

    _assert_no_leakage_and_no_cause(exc_info.value)
