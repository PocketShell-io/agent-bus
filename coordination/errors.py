class CoordinationError(Exception):
    """Base error for the SSH relay. Never includes credential material."""


class UnknownDevice(CoordinationError):
    def __init__(self, device_id: str):
        super().__init__(f"unknown_device:{device_id}")
        self.device_id = device_id
        self.code = "unknown_device"


class UnregisteredAlias(CoordinationError):
    def __init__(self, alias: str):
        super().__init__(f"unregistered_alias:{alias}")
        self.alias = alias
        self.code = "unregistered_alias"


class CatalogStale(CoordinationError):
    def __init__(self, reason: str):
        super().__init__(f"catalog_stale:{reason}")
        self.code = "catalog_stale"


class GuardRejected(CoordinationError):
    def __init__(self, reason: str):
        super().__init__(f"guard_rejected:{reason}")
        self.reason = reason
        self.code = "guard_rejected"


class IdempotencyConflict(CoordinationError):
    def __init__(self, key: str):
        super().__init__(f"idempotency_conflict:{key}")
        self.key = key
        self.code = "idempotency_conflict"


class TransportUnavailable(CoordinationError):
    def __init__(self, reason: str):
        super().__init__(f"transport_unavailable:{reason}")
        self.code = "transport_unavailable"


class NativeBindingMissing(CoordinationError):
    def __init__(self, device_id: str):
        super().__init__(f"native_binding_missing:{device_id}")
        self.device_id = device_id
        self.code = "native_binding_missing"


class TransportError(CoordinationError):
    """Raised when the remote transport exits non-zero or connection fails.

    Per C2166: Completely omits raw remote stdout and stderr strings from public
    exception messages and exception attributes. Carries only bounded, structured,
    sanitized error codes and exit code.
    """

    def __init__(
        self,
        message: str = "remote_transport_failed",
        exit_code: int | None = None,
        code: str = "remote_transport_failed",
    ):
        super().__init__(message)
        self.exit_code = exit_code
        self.code = code


class TransportTimeout(TransportError):
    """Raised when the remote command execution exceeds the timeout limit.

    Per C2164 / C2166: Represents an unknown remote state with zero automated mutating retries.
    Completely omits raw stdout/stderr to prevent traceback leaks.
    """

    def __init__(
        self,
        message: str = "transport_timeout",
        timeout_sec: float | None = None,
        code: str = "transport_timeout",
    ):
        super().__init__(message=message, exit_code=None, code=code)
        self.timeout_sec = timeout_sec


class FramingError(CoordinationError):
    """Raised when remote output is corrupted, non-JSON, or malformed framing.

    Per C2166: Carries only bounded, structured error reasons. Raw remote output
    and JSONDecodeError doc strings are completely omitted.
    """

    def __init__(
        self,
        message: str = "invalid_json_framing",
        reason: str = "invalid_json_framing",
        code: str = "framing_error",
    ):
        super().__init__(message)
        self.reason = reason
        self.code = code


class AuthError(CoordinationError):
    """Raised when authentication or authorization fails on the remote FileBus.

    Per C2166: Carries only bounded, structured reasons. Never embeds tokens or details.
    """

    def __init__(
        self,
        message: str = "remote_auth_rejected",
        reason: str = "remote_auth_rejected",
        code: str = "auth_error",
    ):
        super().__init__(message)
        self.reason = reason
        self.code = code
