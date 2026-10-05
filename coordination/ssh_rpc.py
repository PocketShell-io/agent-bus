"""Typed SSH FileBus RPC Client.

Communicates with a remote FileBus store over SSH using pinned CLI execution
and safe JSON streams over stdin/stdout (no shell interpolation, no credentials in argv).

Hardened per Codex Principal Directives C2162, C2164, and C2166:
- Local SSH Option Injection Defense: validates host does not start with '-', passes '--' before host.
- Remote Shell Escaping (POSIX Scope): shlex.quote applied to all remote arguments for POSIX login shells.
  Windows remote shells (cmd.exe / PowerShell) remain held out-of-scope.
- Mandatory StrictHostKeyChecking & BatchMode: enforces -o BatchMode=yes and -o StrictHostKeyChecking=yes
  (prohibits ambient accept-new or no).
- Fail-Closed Omission: raw remote stdout and stderr strings are completely omitted from exception
  messages and attributes to eliminate credential and payload leaks.
- Decoupled Exception Chaining: raises TransportTimeout and FramingError using `raise ... from None`
  outside except blocks, preventing traceback leaks via __cause__ / __context__.
- Strict Correlation & Response Semantics: assert resp.request_id == req.request_id, raise
  FramingError("request_id_mismatch") from None without printing payload. Strict boolean type on resp.ok.
- Strict Exit Code Check: non-zero exit codes fail closed with TransportError.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
import shlex
import subprocess
from typing import Any, Callable

from coordination.envelope import NamespacedId, RpcRequest, RpcResponse, new_request_id
from coordination.errors import (
    AuthError,
    CoordinationError,
    FramingError,
    IdempotencyConflict,
    TransportError,
    TransportTimeout,
)

RunnerFunc = Callable[[list[str], str | None, float], tuple[int, str, str]]


def _redact(text: str) -> str:
    """Redacts tokens, credentials, and message bodies from text strings (utility helper)."""
    if not text:
        return ""
    t = re.sub(
        r'("(?:token|parent_token|password|secret|body)":\s*")[^"]+(")',
        r'\1[REDACTED]\2',
        text,
    )
    t = re.sub(r'(token=)[^\s&]+', r'\1[REDACTED]', t)
    t = re.sub(r'(bearer\s+)[a-zA-Z0-9_\-\.]+', r'\1[REDACTED]', t, flags=re.IGNORECASE)
    return t


def _validate_host(host: str) -> str:
    """Validates host against SSH option injection (e.g., -oProxyCommand=...)."""
    if not host or not host.strip():
        raise ValueError("Host parameter must be non-empty")
    h = host.strip()
    if h.startswith("-"):
        raise ValueError(f"Host parameter must not start with '-': {h}")
    return h


def default_subprocess_runner(cmd: list[str], stdin_data: str | None, timeout: float) -> tuple[int, str, str]:
    """Default execution runner using subprocess.Popen without shell."""
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE if stdin_data is not None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = proc.communicate(input=stdin_data, timeout=timeout)
        return proc.returncode, stdout, stderr
    except subprocess.TimeoutExpired as exc:
        proc.kill()
        proc.wait(timeout=2.0)
        raise exc


def _split_option(opt_str: str) -> tuple[str, str]:
    """Splits an OpenSSH option string on whitespace or '=' into (keyword.lower(), value)."""
    opt_str = opt_str.strip()
    if "=" in opt_str:
        k, v = opt_str.split("=", 1)
        return k.strip().lower(), v.strip()
    parts = opt_str.split(None, 1)
    if len(parts) == 2:
        return parts[0].strip().lower(), parts[1].strip()
    if len(parts) == 1:
        return parts[0].strip().lower(), ""
    return "", ""


ALLOWED_SSH_OPTION_KEYS: set[str] = {
    "batchmode",
    "stricthostkeychecking",
    "connecttimeout",
    "serveraliveinterval",
    "serveralivecountmax",
    "tcpkeepalive",
    "compression",
    "port",
    "user",
    "identityfile",
    "identitiesonly",
    "ciphers",
    "macs",
    "kexalgorithms",
    "hostkeyalgorithms",
}

CUSTOM_TRUST_OPTION_KEYS: set[str] = {
    "proxycommand",
    "proxyjump",
}


def _parse_and_validate_ssh_opts(
    opts: list[str],
    allow_custom_proxycommand: bool = False,
) -> list[str]:
    """
    Parses and validates OpenSSH options to prevent option injection and policy bypass.

    Directives (C2162, C2166, C2171, C2181, C2185, C2186):
    - Normalization: case-insensitive parsing of option keywords and values (e.g. stricthostkeychecking=no, batchmode=no).
    - Spacing & tokenization: handles attached (-oKey=Val) and detached (-o, Key=Val or -o, Key, Val) forms, spaces around '='.
    - Strict enforcement: BatchMode=yes and StrictHostKeyChecking=yes are mandatory.
      Values like 'accept-new', 'no', 'off', 'ask' strictly fail closed with ValueError.
    - Precedence & duplicate handling: inspects ALL occurrences of options.
      If conflicting values are present, non-'yes' values fail closed immediately.
    - Default-deny option allowlist (C2185 / C2186): only vetted option keys (ALLOWED_SSH_OPTION_KEYS)
      are permitted. Arbitrary directives (KnownHostsCommand, Include, LocalCommand, PermitLocalCommand,
      Match, PKCS11Provider) strictly fail closed with ValueError.
    - Untrusted directive defense: ProxyCommand and ProxyJump are rejected unless explicit custom trust
      (allow_custom_proxycommand=True) is granted.
    - Narrow flag allowlist (C2181): only allowed option flags (-o, -p, -i, -F, -4, -6, -C, -q, -v, -vv, -vvv, -T, -N, -n).
      Unrecognized flags fail closed.
    - Positional destination defense (C2181): bare positional arguments in ssh_opts (tokens not starting with '-')
      are strictly prohibited to prevent preceding destination hijacking before '--'.
    - Config file defense (C2181): '-F' custom config files are prohibited unless allow_custom_proxycommand=True,
      preventing ambient configuration files from re-introducing LocalCommand or ProxyCommand.
    """
    normalized_other: list[str] = []
    seen_options: dict[str, list[str]] = {}

    i = 0
    while i < len(opts):
        arg = opts[i]
        if not arg or not arg.strip():
            i += 1
            continue

        # 1. OpenSSH -o option processing
        if arg == "-o":
            if i + 1 >= len(opts):
                raise ValueError("-o flag requires an option argument")
            opt_str = opts[i + 1]
            i += 2
            k, v = _split_option(opt_str)
            if not v and i < len(opts) and not opts[i].startswith("-"):
                k, v = opt_str.strip().lower(), opts[i].strip()
                i += 1
            seen_options.setdefault(k, []).append(v)
            v_lower = v.lower()

            if k in CUSTOM_TRUST_OPTION_KEYS:
                if not allow_custom_proxycommand:
                    raise ValueError(
                        f"OpenSSH directive '{k}' is untrusted and prohibited without explicit custom trust (allow_custom_proxycommand=True)"
                    )
            elif k not in ALLOWED_SSH_OPTION_KEYS:
                raise ValueError(
                    f"OpenSSH option '{k}' is prohibited; only vetted options in allowlist are permitted (prohibiting directives like KnownHostsCommand, Include, LocalCommand)"
                )

            if k == "stricthostkeychecking":
                if v_lower != "yes":
                    raise ValueError(
                        f"StrictHostKeyChecking must be 'yes' (found '{v}'); StrictHostKeyChecking={v} is prohibited"
                    )
            elif k == "batchmode":
                if v_lower != "yes":
                    raise ValueError(
                        f"BatchMode must be 'yes' (found '{v}'); BatchMode={v} is prohibited"
                    )
            continue

        elif arg.startswith("-o"):
            opt_str = arg[2:].lstrip()
            i += 1
            k, v = _split_option(opt_str)
            if not v and i < len(opts) and not opts[i].startswith("-"):
                k, v = opt_str.strip().lower(), opts[i].strip()
                i += 1
            seen_options.setdefault(k, []).append(v)
            v_lower = v.lower()

            if k in CUSTOM_TRUST_OPTION_KEYS:
                if not allow_custom_proxycommand:
                    raise ValueError(
                        f"OpenSSH directive '{k}' is untrusted and prohibited without explicit custom trust (allow_custom_proxycommand=True)"
                    )
            elif k not in ALLOWED_SSH_OPTION_KEYS:
                raise ValueError(
                    f"OpenSSH option '{k}' is prohibited; only vetted options in allowlist are permitted (prohibiting directives like KnownHostsCommand, Include, LocalCommand)"
                )

            if k == "stricthostkeychecking":
                if v_lower != "yes":
                    raise ValueError(
                        f"StrictHostKeyChecking must be 'yes' (found '{v}'); StrictHostKeyChecking={v} is prohibited"
                    )
            elif k == "batchmode":
                if v_lower != "yes":
                    raise ValueError(
                        f"BatchMode must be 'yes' (found '{v}'); BatchMode={v} is prohibited"
                    )
            continue

        # 2. OpenSSH -F custom config processing (C2181)
        elif arg == "-F" or arg.startswith("-F"):
            if not allow_custom_proxycommand:
                raise ValueError(
                    "OpenSSH config file option '-F' is untrusted and prohibited without explicit custom trust (allow_custom_proxycommand=True)"
                )
            if arg == "-F":
                if i + 1 >= len(opts):
                    raise ValueError("-F flag requires a config path argument")
                cfg_val = opts[i + 1]
                if cfg_val.startswith("-"):
                    raise ValueError(f"Config path for '-F' must not start with '-': {cfg_val}")
                normalized_other.extend(["-F", cfg_val])
                i += 2
            else:
                cfg_val = arg[2:].lstrip()
                if not cfg_val or cfg_val.startswith("-"):
                    raise ValueError(f"Config path for '-F' must not start with '-': {cfg_val}")
                normalized_other.append(arg)
                i += 1
            continue

        # 3. Known allowed value flags: -p, -i, -l, -c
        elif arg in ("-p", "-i", "-l", "-c"):
            if i + 1 >= len(opts):
                raise ValueError(f"SSH flag '{arg}' requires an argument")
            val = opts[i + 1]
            if val.startswith("-"):
                raise ValueError(f"Argument for SSH flag '{arg}' must not start with '-': {val}")
            if arg == "-p":
                try:
                    port = int(val)
                    if not (1 <= port <= 65535):
                        raise ValueError()
                except (ValueError, TypeError):
                    raise ValueError(f"SSH port flag '-p' requires a valid integer port between 1 and 65535 (got '{val}')")
            normalized_other.extend([arg, val])
            i += 2
            continue

        elif any(arg.startswith(prefix) for prefix in ("-p", "-i", "-l", "-c")) and len(arg) > 2:
            prefix = arg[:2]
            val = arg[2:]
            if prefix == "-p":
                try:
                    port = int(val)
                    if not (1 <= port <= 65535):
                        raise ValueError()
                except (ValueError, TypeError):
                    raise ValueError(f"SSH port flag '-p' requires a valid integer port between 1 and 65535 (got '{val}')")
            normalized_other.append(arg)
            i += 1
            continue

        # 4. Known allowed boolean flags
        elif arg in ("-4", "-6", "-C", "-q", "-v", "-vv", "-vvv", "-T", "-N", "-n"):
            normalized_other.append(arg)
            i += 1
            continue

        # 5. Any bare positional argument or unrecognized flag fails closed!
        else:
            if not arg.startswith("-"):
                raise ValueError(
                    f"Positional destination argument in ssh_opts is prohibited (destinations must use host parameter): '{arg}'"
                )
            else:
                raise ValueError(f"Prohibited or unrecognized SSH option flag: '{arg}'")

    # Mandatory canonical options: BatchMode=yes and StrictHostKeyChecking=yes
    out: list[str] = ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes"]
    for k, vals in seen_options.items():
        if k not in ("batchmode", "stricthostkeychecking"):
            for val in vals:
                out.extend(["-o", f"{k}={val}"])
    out.extend(normalized_other)
    return out


@dataclass
class SshFileBusClient:
    """
    Typed client for interacting with a remote FileBus store over SSH.

    Guarantees:
    - Pinned CLI execution: executes python3 <bus_cli_path> --store <store_path> rpc via ssh (argv list, shell=False).
    - Host option injection defense: validates host does not start with '-', uses '--' before host in argv.
    - Remote shell escaping: shlex.quote applied to all remote command arguments for POSIX login shells.
    - Mandatory SSH hardening: enforces -o BatchMode=yes and -o StrictHostKeyChecking=yes.
    - Zero command-line injection: payloads and tokens are passed strictly via stdin JSON streams.
    - Zero token leakage in argv: credentials never appear in process argument lists.
    - Fail-closed omission: completely omits raw stdout/stderr strings from exceptions and attributes.
    - Decoupled exception chaining: raises with `from None` to avoid leaks via traceback __cause__ / __context__.
    - Strict exit code check: rc != 0 fails closed with TransportError.
    - Strict correlation: request_id must match (request_id_mismatch) and ok must be strict bool.
    - Clean response framing: no arbitrary banner picking.
    """

    host: str
    store_path: str
    bus_cli_path: str = "coordination/bus_cli.py"
    ssh_binary: str = "ssh"
    ssh_opts: list[str] = field(
        default_factory=lambda: [
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=yes",
        ]
    )
    timeout_sec: float = 30.0
    allow_custom_proxycommand: bool = False
    runner: RunnerFunc | None = None

    def __post_init__(self) -> None:
        self.host = _validate_host(self.host)
        self.ssh_opts = _parse_and_validate_ssh_opts(
            self.ssh_opts,
            allow_custom_proxycommand=self.allow_custom_proxycommand,
        )

    def _get_runner(self) -> RunnerFunc:
        return self.runner if self.runner is not None else default_subprocess_runner

    def _execute_rpc(self, req: RpcRequest) -> RpcResponse:
        """
        Executes a typed RPC request against the remote FileBus over SSH.

        POSIX Scope Boundary (C2164 / C2166):
        Remote shell invocation strictly assumes a POSIX-compliant login shell
        ($SHELL, /bin/sh, /bin/bash, /bin/dash, /bin/zsh). Arguments are escaped
        using shlex.quote() before remote shell concatenation. Windows remote shells
        (cmd.exe / PowerShell) have incompatible quoting and remain out-of-scope.

        Decoupled Exception Chaining & Fail-Closed Omission (C2166):
        Raw remote stdout and stderr strings are completely omitted from exception
        messages and exception attributes. Subprocess and JSON decode exceptions
        are decoupled using `raise ... from None` outside except blocks to eliminate
        leakage via tracebacks (__cause__ and __context__ both None).
        """
        remote_cmd_parts = [
            "python3",
            shlex.quote(self.bus_cli_path),
            "--store",
            shlex.quote(self.store_path),
            "rpc",
        ]
        # Command line uses '--' before host to prevent SSH option injection.
        # StrictHostKeyChecking=yes and BatchMode=yes are enforced.
        cmd = [self.ssh_binary] + list(self.ssh_opts) + ["--", self.host] + remote_cmd_parts
        stdin_payload = json.dumps(req.to_dict())
        runner = self._get_runner()

        timeout_err: TransportTimeout | None = None
        coordination_err: CoordinationError | None = None
        generic_err: TransportError | None = None
        rc: int = 0
        stdout: str = ""
        stderr: str = ""

        try:
            rc, stdout, stderr = runner(cmd, stdin_payload, self.timeout_sec)
        except subprocess.TimeoutExpired:
            # Per C2164 / C2166: Timeout represents an unknown remote state.
            # Automated mutating retries are prohibited.
            timeout_err = TransportTimeout(
                f"SSH RPC timed out after {self.timeout_sec}s",
                timeout_sec=self.timeout_sec,
            )
        except Exception as exc:
            if isinstance(exc, CoordinationError):
                coordination_err = exc
            else:
                generic_err = TransportError(
                    "SSH execution failed",
                    exit_code=None,
                    code="remote_transport_failed",
                )

        if timeout_err is not None:
            raise timeout_err from None
        if coordination_err is not None:
            raise coordination_err from None
        if generic_err is not None:
            raise generic_err from None

        # Strict exit code check: any non-zero exit code fails closed with TransportError
        # Per C2166: Raw stdout and stderr are completely omitted.
        if rc != 0:
            raise TransportError(
                f"SSH command failed with exit {rc}",
                exit_code=rc,
                code="remote_transport_failed",
            )

        # Clean response framing check: stdout must parse directly as clean JSON RpcResponse
        raw_stdout = stdout.strip()
        if not raw_stdout:
            raise FramingError(
                "Remote host returned empty output",
                reason="empty_output",
            )

        json_decode_err: FramingError | None = None
        resp_obj = None
        try:
            resp_obj = json.loads(raw_stdout)
        except json.JSONDecodeError:
            # Per C2166: Decouple exception chaining with `raise ... from None`
            # and NEVER embed raw stdout or err.doc in the exception message.
            json_decode_err = FramingError(
                "Failed to parse valid RPC response framing from remote host",
                reason="invalid_json_framing",
            )

        if json_decode_err is not None:
            raise json_decode_err from None

        if not isinstance(resp_obj, dict):
            raise FramingError(
                f"Expected JSON object in RPC response framing, got {type(resp_obj).__name__}",
                reason="invalid_json_type",
            )

        validation_err: FramingError | None = None
        resp = None
        try:
            resp = RpcResponse.from_dict(resp_obj)
        except ValueError as exc:
            err_msg = str(exc)
            if "strict boolean" in err_msg or "boolean" in err_msg:
                msg = "RPC response 'ok' field must be a strict boolean"
                reason = "invalid_bool_type"
            else:
                msg = "RPC response framing validation failed"
                reason = "schema_validation_failed"
            validation_err = FramingError(msg, reason=reason)

        if validation_err is not None:
            raise validation_err from None

        # Strict correlation check (C2162 / C2166):
        # If resp.request_id != req.request_id, raise FramingError("request_id_mismatch") from None
        # without printing the payload.
        if resp.request_id != req.request_id:
            raise FramingError(
                "request_id_mismatch",
                reason="request_id_mismatch",
            ) from None

        # Strict boolean check for ok (reject string 'false' or non-boolean truthy values)
        if type(resp.ok) is not bool:
            raise FramingError(
                "RPC response 'ok' field must be a strict boolean",
                reason="invalid_bool_type",
            )

        if not resp.ok:
            err = resp.error or {}
            err_code = str(err.get("code", "unknown_error"))
            if err_code in ("auth_failed", "unauthorized", "invalid_token", "permission_denied", "not_recipient"):
                raise AuthError(
                    f"Remote authentication error ({err_code})",
                    reason="remote_auth_rejected",
                )
            if err_code == "idempotency_conflict":
                raise IdempotencyConflict(f"idempotency_conflict:{err_code}")
            raise CoordinationError(f"Remote FileBus error ({err_code})")

        return resp

    def enroll(
        self,
        agent_name: str,
        device_id: str,
        project_id: str = "agent-coordination",
        task_id: str | None = None,
        parent_id: str | None = None,
        parent_token: str | None = None,
    ) -> tuple[str, str]:
        """
        Register / enroll an agent identity with the remote FileBus store.
        Returns: (identity_id, token).
        """
        params: dict[str, Any] = {
            "agent_name": agent_name,
            "device_id": device_id,
            "project_id": project_id,
        }
        if task_id is not None:
            params["task_id"] = task_id
        if parent_id is not None:
            params["parent_id"] = parent_id
        if parent_token is not None:
            params["parent_token"] = parent_token

        req = RpcRequest(
            op="enroll",
            request_id=new_request_id(),
            params=params,
        )
        resp = self._execute_rpc(req)
        result = resp.result or {}
        return str(result["identity_id"]), str(result["token"])

    def send(
        self,
        sender_id: str,
        token: str,
        recipient_id: str,
        body: str,
        data: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        """
        Send a message to a recipient identity over the remote FileBus.
        Returns: message_id.
        """
        params: dict[str, Any] = {
            "sender_id": sender_id,
            "token": token,
            "recipient_id": recipient_id,
            "body": body,
        }
        if data is not None:
            params["data"] = data
        if idempotency_key is not None:
            params["idempotency_key"] = idempotency_key

        req = RpcRequest(
            op="send",
            request_id=new_request_id(),
            params=params,
        )
        resp = self._execute_rpc(req)
        result = resp.result or {}
        return str(result["message_id"])

    def inbox(
        self,
        identity_id: str,
        token: str,
        unread_only: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Retrieve messages for identity_id from the remote FileBus inbox.
        Returns: list of message dicts.
        """
        params: dict[str, Any] = {
            "identity_id": identity_id,
            "token": token,
            "unread_only": unread_only,
        }
        req = RpcRequest(
            op="inbox",
            request_id=new_request_id(),
            params=params,
        )
        resp = self._execute_rpc(req)
        return list(resp.result or [])

    def ack(
        self,
        identity_id: str,
        token: str,
        message_id: str,
    ) -> dict[str, Any]:
        """
        Acknowledge receipt of a message on the remote FileBus.
        Returns: updated message public dict.
        """
        params: dict[str, Any] = {
            "identity_id": identity_id,
            "token": token,
            "message_id": message_id,
        }
        req = RpcRequest(
            op="ack",
            request_id=new_request_id(),
            params=params,
        )
        resp = self._execute_rpc(req)
        return dict(resp.result or {})

    def reply(
        self,
        sender_id: str,
        token: str,
        message_id: str,
        body: str,
        data: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        """
        Reply to a previously received message on the remote FileBus.
        Returns: reply message_id.
        """
        params: dict[str, Any] = {
            "sender_id": sender_id,
            "token": token,
            "message_id": message_id,
            "body": body,
        }
        if data is not None:
            params["data"] = data
        if idempotency_key is not None:
            params["idempotency_key"] = idempotency_key

        req = RpcRequest(
            op="reply",
            request_id=new_request_id(),
            params=params,
        )
        resp = self._execute_rpc(req)
        result = resp.result or {}
        return str(result["message_id"])

    def get(
        self,
        identity_id: str,
        token: str,
        message_id: str,
    ) -> dict[str, Any]:
        """
        Fetch a specific message by ID.
        Returns: message public dict.
        """
        params: dict[str, Any] = {
            "identity_id": identity_id,
            "token": token,
            "message_id": message_id,
        }
        req = RpcRequest(
            op="get",
            request_id=new_request_id(),
            params=params,
        )
        resp = self._execute_rpc(req)
        return dict(resp.result or {})
