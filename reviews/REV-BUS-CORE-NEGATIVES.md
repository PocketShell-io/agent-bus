# REV-BUS-CORE-NEGATIVES — Independent Code and Negative-Tests Review

Verdict: **ACCEPT**

- **Reviewer**: Antigravity Subagent (`09649355-7fa8-4f33-a545-e1fbc90f750d`)
- **Review Role**: Independent Code and Negative-Tests Reviewer for Agent Bus core `send()` repairs
- **Parent / Head**: `agent-coordination-head` (`81e8010c-89e4-478b-be3a-4ee6991607f3`) via caller `764358a8-1b4e-49c6-845a-9b79bf3ba536`
- **Workspace**: `/home/alexey/git/agent-bus`
- **Predecessor**: `6168aae7-0f3d-44b1-a749-6e195235b079`
- **Successor Repair Under Review**: `207a93f9-83ae-4157-9272-01c384039146` (status `AC-CORE-NEG-FIXED` recorded in `.local/ac-bus-core-status.json`)
- **Review Date**: 2026-10-05

---

## 1. Evaluated Commit and Working Tree State

- **Base Commit**: `f3295f9` (*"Pin independent FileBus review ACCEPT_WITH_FIXES."*)
- **Working Tree State**:
  - Modified files:
    - `coordination/__init__.py`
    - `coordination/bus.py`
    - `coordination/bus_cli.py`
    - `coordination/cursors.py`
    - `tests/test_bus.py`
    - `tests/test_bus_dogfood.py`
  - Added / Untracked files under review:
    - `coordination/durable.py`
    - `coordination/headless_worker.py`
    - `tests/test_bus_concurrent.py`
    - `tests/test_bus_crash.py`
    - `tests/test_bus_scope.py`
    - `tests/test_headless_task.py`
  - Untracked local metadata preserved in `.local/`:
    - `.local/ac-bus-core-status.json` (`AC-CORE-NEG-FIXED`)
    - `.local/ac-bus-core-first-action.json`
    - `.local/ac-bus-core-repair-first-action.json`
    - `.local/ac-bus-core-ack.json`

---

## 2. Verification of the 3 Live Root Negative Cases

The 3 negative cases were independently analyzed in `coordination/bus.py`, traced against unit tests in `tests/test_bus.py`, and verified with an independent test run.

### Negative Case 1: Unrelated identity `send(kind="reply", reply_to=...)` rejected with `not_recipient` unless sender is original recipient
- **Implementation**: `coordination/bus.py` lines 178–205 (`_require_reply_target_locked`) invoked by `_send_locked` (lines 298–305) and `reply` (lines 433–460):
  ```python
  if kind == "reply" and original["recipient_id"] != sender_id:
      raise BusError("not_recipient", reply_to)
  ```
- **Test in Suite**: `tests/test_bus.py::test_send_kind_reply_rejects_unrelated_same_project_identity`
- **Verified Assertions**:
  1. Identity `c` attempting `bus.send(sender_id=c.identity_id, recipient_id=a.identity_id, kind="reply", reply_to=msg.message_id)` raises `BusError` with `err.value.code == "not_recipient"`.
  2. Inbox of original sender `a` is not polluted: `bus.inbox(a.identity_id, ta) == []`.
  3. Legitimate reply by original recipient `b` succeeds both via `bus.reply()` and direct `bus.send(kind="reply", reply_to=msg.message_id)`.
  4. Original sender `a` cannot reply to its own outgoing message using `kind="reply"` (raises `not_recipient`).

### Negative Case 2: Different-project `reply_to` rejected with `project_scope`
- **Implementation**: `coordination/bus.py` lines 178–205 (`_require_reply_target_locked`):
  ```python
  self._require_same_project(sender, orig_sender)
  self._require_same_project(sender, orig_recipient)
  ```
  If `orig_sender` or `orig_recipient` has a different `project_id` from the sending identity, `_require_same_project` raises `BusError("project_scope", ...)`.
- **Test in Suite**: `tests/test_bus.py::test_send_reply_to_rejects_different_project_message`
- **Verified Assertions**:
  1. Identity `c` in project `beta` attempting `bus.send(..., reply_to=msg.message_id)` targeting project `alpha`'s message raises `BusError` with `err.value.code == "project_scope"`.
  2. Recipient `d`'s inbox is empty: `bus.inbox(d.identity_id, td) == []`.
  3. Identity `c` attempting `bus.send(..., kind="reply", reply_to=msg.message_id)` also raises `BusError` with `err2.value.code == "project_scope"`.
  4. Cross-project message inspection via `restarted.get(...)` raises `project_scope` (tested in `tests/test_bus_scope.py::test_inbox_hides_other_project_even_if_file_is_planted`).

### Negative Case 3: Changed `kind` or `reply_to` on reused idempotency key raises `IdempotencyConflict`
- **Implementation**: `coordination/bus.py` lines 287–297 in `_send_locked`:
  ```python
  for existing in messages.values():
      if existing.get("idempotency_key") == key:
          if (
              existing["sender_id"] == sender_id
              and existing["recipient_id"] == recipient_id
              and existing.get("digest") == digest
              and existing.get("kind") == kind
              and existing.get("reply_to") == reply_to
          ):
              return _msg(existing)
          raise IdempotencyConflict(key)
  ```
- **Test in Suite**: `tests/test_bus.py::test_idempotent_send_conflict_when_kind_or_reply_to_change`
- **Verified Assertions**:
  1. Reusing key `"k"` with changed `kind="reply"` and `reply_to=original.message_id` raises `IdempotencyConflict`.
  2. Reusing key `"k"` with changed `reply_to=original.message_id` but same `kind="note"` raises `IdempotencyConflict`.
  3. Replay with identical parameters (`sender_id`, `recipient_id`, `digest`, `kind`, `reply_to`) succeeds idempotently and returns the identical `BusMessage` (`replay.message_id == first.message_id`).
  4. Inbox contains exactly 1 delivered message.

---

## 3. Durability, fsync, and umask 077 Verification

1. **fsync and directory fsync**:
   - `coordination/durable.py`:
     - `write_all(fd, payload)` loops until all bytes are written to handle partial POSIX `write()`.
     - `atomic_write_json(path, value)` writes to temporary file (`.tmp`), invokes `os.fsync(fd)`, renames atomically via `os.replace(tmp, path)`, and invokes `fsync_dir(path.parent)` to ensure directory entries are committed to persistent storage.
     - `FileBus._commit` writes to `journal.json` WAL with `atomic_write_json`, writes data files with `atomic_write_json`, unlinks `journal.json`, and invokes `fsync_dir(self.root)`.
     - Tested in `tests/test_bus_crash.py` (`test_write_retries_shortwrite`, `test_write_fsyncs_file_and_directory`, `test_journal_replay_completes_register`, `test_orphan_identity_without_token_is_dropped`).
2. **Umask 077 and File Permissions**:
   - Store directories created with explicit `mode=0o700` (`mkdir(parents=True, exist_ok=True, mode=0o700)`).
   - Data files created with `os.open(tmp, flags, mode=0o600)` and lock files with `0o600`.
   - Credential files written via `write_secret_json` explicitly execute `os.chmod(path, 0o600)`.
   - Independently tested under both `umask 077` and `umask 000`:
     - Store directory: `0o700`
     - All files (`bus.lock`, `messages.json`, `identities.json`, `tokens.json`, `cursors/idempotency.json`, `cursors/cursors.lock`): strictly `0o600`.
     - Subdirectory `cursors/`: `0o700`.

---

## 4. Test Execution Results

Executed standard command:
```bash
PYTHONPATH=/home/alexey/git/agent-bus PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python3 -m pytest tests -v
```

Output:
```text
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.1.1, pluggy-1.6.0
rootdir: /home/alexey/git/agent-bus
configfile: pyproject.toml
collecting ... collected 24 items

../agent-bus/tests/test_bus.py ...........                               [ 45%]
../agent-bus/tests/test_bus_concurrent.py ..                             [ 54%]
../agent-bus/tests/test_bus_crash.py .....                               [ 75%]
../agent-bus/tests/test_bus_dogfood.py .                                 [ 79%]
../agent-bus/tests/test_bus_scope.py ....                                [ 95%]
../agent-bus/tests/test_headless_task.py .                               [100%]

============================== 24 passed in 5.71s ==============================
```

All 24 test cases pass cleanly without errors, warnings, or skips.

---

## 5. Verification of No Fake Mocks or Synthetic Bypasses

- **No Synthetic Bypasses**: The test suite does not bypass `FileBus` logic or fake return values.
- **Real File and Subprocess Operations**:
  - `tests/test_bus_dogfood.py` and `tests/test_headless_task.py` spawn genuine `sys.executable` child processes executing `coordination/bus_cli.py` and `coordination/headless_worker.py`.
  - Storage is written to real temporary filesystem paths (`tmp_path`) with real JSON files and real POSIX file locks (`fcntl.flock`).
- **Monkeypatching Scope**: The only monkeypatches in the test suite are:
  1. `test_identity_is_not_aplexer_session`: sets `APLEXER_SESSION_ID` to prove that the bus does *not* adopt the external session id.
  2. `test_bus_crash.py`: fault-injection harnesses that simulate POSIX short writes or spy on `os.open`/`os.fsync` calls without altering write behavior.

---

## 6. Residual Risks and Observations

1. **Same-User OS Trust Boundary**:
   As documented in `coordination/bus.py` and `coordination/durable.py`, filesystem permissions (`0700`/`0600`) protect the store from other OS users. Agents running under the same UID with access to the store path share filesystem access. The bearer token checks and project scope validations are application-level accident barriers, not a defense against hostile processes under the same UID.
2. **Idempotency Key Scan Complexity**:
   `_send_locked` iterates `messages.values()` to detect existing idempotency keys. For large message stores, an in-memory or on-disk index map should be preferred to maintain O(1) key lookups.
3. **Local Store Scope**:
   `FileBus` uses local POSIX file locking (`fcntl.flock`) and local directory fsyncs. It is designed for multi-agent coordination on a single host. Cross-host coordination requires the network adapter / bridge layer.

---

## 7. Independent Verdict

**ACCEPT**

The 3 live root negative cases are cleanly enforced and thoroughly tested:
1. `send(kind=reply, reply_to)` rejects unrelated senders with `not_recipient`.
2. Different-project `reply_to` is strictly rejected with `project_scope`.
3. Reusing an idempotency key with altered `kind` or `reply_to` reliably raises `IdempotencyConflict`.

Durable fsync, directory fsync, POSIX permissions, crash recovery WAL, and headless child process integration are validated and operational.
