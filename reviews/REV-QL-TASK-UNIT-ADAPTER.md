# REV-QL-TASK-UNIT-ADAPTER — Independent Code Review

Verdict: **ACCEPT**

- **Reviewer**: Antigravity Independent Reviewer
- **Review Role**: Independent Code Reviewer for the QL Sessionless Consumer Adapter
- **Caller / Parent**: `764358a8-1b4e-49c6-845a-9b79bf3ba536`
- **Project**: Agent Coordination (`agent-bus`)
- **Workspace**: `/home/alexey/git/agent-bus`
- **Canonical Baseline Pin**: `5ee5207e8c540b712f7bc862af8f3bf29dc6df69`
- **Committed Implementation Pin**: `48ab4fb6519471659211cf611a6c487e68b61c9d` (on `origin/main`)
- **Review Date**: 2026-10-05

---

## 1. Evaluated Commits and Scope

The review evaluated the commit range `5ee5207e8c540b712f7bc862af8f3bf29dc6df69..48ab4fb6519471659211cf611a6c487e68b61c9d`:

- `5ee5207e` (Base): *"Add FileBus core negative fixes, durable storage, envelope validator, and test suites"*
- `6f68903` (Adapter baseline): *"Add maintained Quota Launcher Task-Unit Adapter with 50-GiB storage floor check"*
- `48ab4fb` (Head / Under Review): *"Implement and test QLSessionlessConsumer adapter with lease recovery, idempotency, and 50-GiB floor"*

### Scope and Changed Files
- `coordination/ql_task_unit_adapter.py`: +474 lines. Implements the maintained Quota Launcher task-unit bridge, admission floor checks, head credential leak prevention, FileBus live identity and terminal receipt contracts, and `QLSessionlessConsumer` with 4-state lifecycle, cursor advancing, idempotency replay, and crash lease recovery.
- `tests/test_ql_task_unit_adapter.py`: +359 lines. 12 comprehensive unit and integration tests covering storage floor breach, credential leak rejection, identity contracts, dry-run CLI submission, end-to-end consumer execution, idempotent replays, crash lease recovery, and storage floor holding.

---

## 2. Verification of Safety and Contract Requirements

### 2.1. 50.0 GiB Host Root Admission Floor (`check_storage_floor`)
- **Requirement**: Must strictly enforce the 50.0 GiB host root floor before admitting or submitting any task unit.
- **Implementation**:
  ```python
  MIN_ROOT_FREE_GIB = 50.0

  def check_storage_floor(path: str = "/") -> float:
      total, used, free = shutil.disk_usage(path)
      free_gib = free / (1024 ** 3)
      if free_gib < MIN_ROOT_FREE_GIB:
          raise StorageFloorBreach(
              f"Host root filesystem free space ({free_gib:.2f} GiB) is below required {MIN_ROOT_FREE_GIB:.1f} GiB floor. "
              "Worker task-unit submission halted."
          )
      return free_gib
  ```
- **Verification**:
  - `check_storage_floor()` is invoked in `submit_task_unit()` (line 201) before any subprocess is executed.
  - In `QLSessionlessConsumer.process_message()` (line 335), `check_storage_floor()` is verified immediately after message acceptance, failing closed prior to launching any worker.
  - In `QLSessionlessConsumer.recover_leases()` (line 311), `check_storage_floor()` is evaluated before retrying uncompleted messages.
  - Unit tests verify:
    - Free space below 50.0 GiB raises `StorageFloorBreach` (`test_check_storage_floor_raises_when_below_floor`).
    - Free space at/above 50.0 GiB passes cleanly (`test_check_storage_floor_passes_when_above_floor`).
    - Low-disk conditions hold message leases safely without dropping or acknowledging them (`test_consumer_held_when_storage_floor_breached`).

### 2.2. Head Credential Leak Prevention (`reject_head_cred_inheritance`)
- **Requirement**: Fail closed to prevent head.cred leakage to workers across any payload format.
- **Implementation**:
  - Prohibits worker payloads matching `HEAD_BUS_AGENT` (`"quota-launcher-head"`) or `HEAD_BUS_IDENTITY_ID` (`"ad6251d7-49f8-4f20-b8a1-d5af62412c5c"`).
  - Inspects field keys `cred`, `cred_path`, `credential`, `filebus_cred` for `"filebus/head.cred"`.
  - Recursively walks all nested dictionaries, lists, and strings using `HEAD_CRED_GRANT_RE = re.compile(r"--cred(?:\s+|=)\S*filebus/head\.cred\b", re.IGNORECASE)`.
  - Raises `HeadCredentialLeakError` if any pattern matches.
- **Verification**:
  - Enforced in `format_ql_payload()` and `submit_task_unit()`.
  - Verified by `test_reject_head_cred_inheritance` against direct role impersonation, explicit cred paths, and command-line flags embedded in prompt strings.

### 2.3. Identity and Terminal Receipt Contract (`validate_filebus_live_identity` / `validate_filebus_terminal_receipt`)
- **Requirement**: Adhere strictly to the contract defined in `agent-quota-launcher/launcher/filebus_backend.py`.
- **Implementation**:
  - `validate_filebus_live_identity()` requires `task_id`, `bus_identity`, `task_message_id`, and `provider`. It rejects any native whoami markers (`worker_cgroup`, `workload_cgroup`, `socket_path`) and strictly rejects premature terminal receipt fields (`ack_id`, `reply_id`, `outcome`).
  - `validate_filebus_terminal_receipt()` requires all 7 fields (`task_id`, `bus_identity`, `task_message_id`, `provider`, `ack_id`, `reply_id`, `outcome`), requires `outcome` to be one of `("accepted", "completed", "failed", "blocked")`, and rejects native whoami markers.
- **Verification**:
  - The implementation logic, required field tuples, and whoami exclusions match `launcher/filebus_backend.py` exactly.
  - Verified by `test_validate_filebus_identity_contracts`.
  - Live identity is validated at step 5 of `process_message()`, and terminal receipt is validated at step 10 before acknowledging the message.

### 2.4. 4-State Lifecycle Management
- **Requirement**: Correctly manage and decouple `delivered`, `accepted_at`, `outcome` (with artifact digest), and `acked_at`.
- **Implementation**:
  - `delivered`: Recorded upon initial `bus.send()` into the recipient's inbox.
  - `accepted_at`: Recorded via `bus.accept()` before task execution.
  - `outcome`: Recorded via `bus.complete()` with `status`, `artifact`, and SHA-256 `digest` computed from file bytes via `file_digest()`.
  - `reply`: Dispatched to original sender containing the completed outcome data.
  - `acked_at`: Recorded via `bus.ack()` after outcome and terminal receipt validation.
- **Verification**:
  - Verified end-to-end in `test_consumer_processes_message_end_to_end`, confirming each timestamp and outcome payload is independently populated and queryable on the bus.

### 2.5. Cursor Tracking and Idempotency Replay
- **Requirement**: Prevent duplicate execution on retried messages and advance cursors correctly.
- **Implementation**:
  - `process_message()` inspects `current_msg.outcome`: if an outcome already exists on the message, it bypasses task-unit submission entirely, replays the reply on the bus using the idempotent key `f"{current_msg.idempotency_key}:reply"`, calls `bus.ack()`, advances the mailbox cursor via `bus._cursors.advance()`, and returns `{idempotent_replay: True}`.
  - In normal completion, `bus._cursors.advance()` is invoked after `bus.ack()`.
- **Verification**:
  - Verified by `test_consumer_idempotent_replay`: calling `process_message()` on an already processed message returns the replay result with zero additional calls to `submit_task_unit()`.

### 2.6. Crash Recovery and Lease Management (`recover_leases`)
- **Requirement**: Handle crash recovery safely, distinguishing completed outcomes from pending executions, and respecting storage floors.
- **Implementation**:
  - Scans inbox for unacknowledged messages where `accepted_at` is set.
  - **Case 1 (Outcome exists)**: The previous worker finished and recorded outcome, but the process crashed before replying/acking. Re-dispatches reply, executes `bus.ack()`, advances cursor, and records action `"recovered_ack"`.
  - **Case 2 (No outcome)**: The process crashed while the task was in flight. Checks `check_storage_floor()`. If storage is below 50.0 GiB, catches `StorageFloorBreach` and records `"held_storage_floor"` without crashing or dropping the lease. If storage is sufficient, safely re-processes the task.
- **Verification**:
  - Verified by `test_consumer_recover_leases_after_crash` (recovers reply and acks message).
  - Verified by `test_consumer_held_when_storage_floor_breached` (holds unacknowledged message when storage is below 50 GiB).

### 2.7. Path Traversal Confinement
- **Implementation**:
  - `QLSessionlessConsumer._contained_path()` ensures all artifact paths resolve inside `self.allow_root`, raising `ValueError(f"path_outside_allowed_root: {target}")` on escaping paths.

---

## 3. Test Execution Results

The full test suite was executed in `/home/alexey/git/agent-bus` with plugin autoload disabled:

```bash
env -C /home/alexey/git/agent-bus PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests -v
```

### Output:
```text
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: /home/alexey/git/agent-bus
configfile: pyproject.toml
collecting ... collected 36 items

tests/test_bus.py ...........                                            [ 30%]
tests/test_bus_concurrent.py ..                                          [ 36%]
tests/test_bus_crash.py .....                                            [ 50%]
tests/test_bus_dogfood.py .                                              [ 52%]
tests/test_bus_scope.py ....                                             [ 63%]
tests/test_headless_task.py .                                            [ 66%]
tests/test_ql_task_unit_adapter.py ............                          [100%]

============================= 36 passed in 11.25s ==============================
```

- **Total Test Cases**: 36 passed, 0 failed, 0 skipped.
- **Adapter Unit/Integration Tests**: 12/12 passed (`test_ql_task_unit_adapter.py`).
- **Syntax / Compilation**: `python3 -m py_compile` ran with 0 errors.

---

## 4. Verdict

**ACCEPT**

The implementation in `coordination/ql_task_unit_adapter.py` and its test suite in `tests/test_ql_task_unit_adapter.py` (commits `5ee5207e..48ab4fb`) strictly satisfy all safety gates and protocol contracts:
1. Host root 50.0 GiB floor is strictly enforced before any task submission.
2. `head.cred` inheritance is rejected fail-closed across all payload variants.
3. Live enrollment identity and terminal receipt contracts faithfully mirror `agent-quota-launcher`.
4. 4-state lifecycle (`delivered`, `accepted_at`, `outcome with digest`, `acked_at`) is correctly separated and durable.
5. Idempotent replays and cursor advances prevent duplicate execution.
6. Lease recovery handles pre-reply crashes (`recovered_ack`) and storage floor exhaustion (`held_storage_floor`) safely.
