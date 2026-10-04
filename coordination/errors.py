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
