# REV-BUS-CORE — independent review of bus core (FileBus/CLI)

Verdict: **ACCEPT_WITH_FIXES** (local core only; not cross-host acceptance)

- Reviewer: `ac-bus-reviewer` (`f869e27c-6cfc-483b-bb08-2ca25e09e9d3`), engine `shell`
- Parent/head: `agent-coordination-head` (`81e8010c-89e4-478b-be3a-4ee6991607f3`)
- Task: `independent review of bus core`, mode `review`
- Owned write scope: `reviews/` only (this file). No product edits made.
- First actual tool: `aplexer whoami --json` (provenance: `.local/ac-bus-reviewer-first-action.json`)
- Pinned SHA: `06addf9ec875532feae40479c8e64ea11cb600db` ("Copy scoped FileBus core")
- Intake: `docs/maintainer-intake-20261004.md` (2026-10-04)
- Reviewed: `coordination/bus.py`, `coordination/bus_cli.py`, `coordination/cursors.py`, `coordination/errors.py`, `coordination/__init__.py`, `tests/test_bus.py`, `tests/test_bus_dogfood.py`, `docs/checkpoint.md`, `README.md`, `AGENTS.md`, `.gitignore`, `pyproject.toml`

## Tests I ran

1. `python3 -m pytest tests -v` → **6 passed** (`test_bus.py` 5, `test_bus_dogfood.py` 1), Python 3.12.3, stdlib + pytest only.
2. Manual env-clean headless check (minimal `env`, no `APLEXER_*`, `/usr/bin:/bin` PATH): register two identities → distinct UUIDs → `send --idempotency-key env-1` ok.
3. Manual wrong-identity checks against CLI: tampered token → `BusError: auth_failed`; non-recipient `ack` → `BusError: not_recipient`. Both correctly rejected.
4. Static scans: `rg -i aplexer` (only docs/docstrings, no code dep); `rg getpid/getppid/environ/shutil.which/control.sock` in `coordination/` (no hit except docstring); secret scan over tracked files + `git log -p --all` (no hardcoded tokens/keys/private blocks); `git ls-files` confirms `.local/` untracked, `.gitignore` covers `.local/__pycache__/*.pyc/.venv/`.

## Checklist vs intake

- **No aplexer session/env/PID/executable dependency in core: PASS.** `coordination/` imports are stdlib only (`json/os/time/uuid/dataclasses/datetime/pathlib/argparse/sys/hashlib`). No `aplexer` import, no subprocess of aplexer, no `APLEXER_*`/`os.getpid` read. `FileBus`/`bus_cli.py` work under env-clean run.
- **Identities independent of terminal sessions: PASS.** `BusIdentity` is `uuid4`, `kind="bus-agent"`, `(device_id, project_id, agent_name, task_id)` supplied by caller. `test_identity_is_not_aplexer_session` sets `APLEXER_SESSION_ID` and asserts inequality. Never forges/borrows a native session id.
- **Receipt vs read ACK vs semantic outcome vs artifact: FAIL (main reason for WITH_FIXES).** Core implements only one state transition: `acked_at` (read ACK) + `inbox(unread_only=...)` filtering. There is no delivery-receipt field, no `accept` verb, no outcome record. `reply(kind="reply")` is used as a weak acceptance proxy and the dogfood artifact (`artifact.txt`) is an external file, not recorded on the bus. `README.md:5` claims the four stay separate; the code does not yet model them separately.
- **Idempotency and crash-restart redelivery: PASS with notes.** `send` dedupes on `(idempotency_key, sender, recipient, digest)`; conflict raises `IdempotencyConflict`; `test_idempotent_send_and_conflict` + `test_crash_restart_redelivers_unacked` + dogfood restart-before-ACK all pass. Durability: `FileLock` (flock) + atomic `write→fsync→os.replace`, store dir `0700`, files `0600`. Notes: (a) `FileBus.reply` drops the lock between read-original and `send` (TOCTOU, low risk single-node); (b) `CursorStore._save` has no fsync/lock (weaker than `FileBus._write`); (c) unused `Iterator` import in `bus.py`.
- **Two-process dogfood is real processes, not mocks: PASS (local only).** `test_bus_dogfood.py` spawns real `sys.executable coordination/bus_cli.py` subprocesses for register/send/inbox/ack/reply, writes a real `artifact.txt`, simulates B-crash-before-ACK by re-invoking the CLI, and verifies restart redelivery. No mocks/fakes. Correctly scoped: same-host same-filesystem store; `docs/checkpoint.md` + `reviews/README.md` explicitly disclaim cross-host acceptance.
- **Public repo contains no secrets: PASS.** No credential material in `git ls-files` output or `git log -p --all`. Store tokens live in untracked runtime dirs; `.gitignore` excludes `.local/`. One hygiene fix below (CLI cred file perms).
- **Local tests are not cross-host proof: PASS (disclaimed).** Suite is localhost + shared filesystem; no sockets/TLS/multi-machine, no host-bridge vs originating-agent distinction, no draft/busy guard. Checkpoint/README/reviews README state this honestly. No deployed/runtime guarantee is claimed — consistent with AGENTS.md.

## Required fixes before claiming the milestone

1. Model the four states separately (or narrow the claim): add explicit delivery-receipt vs `acked_at` vs `accept` vs `outcome` (e.g. `delivered_at`/`accepted_at`/`outcome` fields or separate records), and record the artifact digest/outcome on the bus instead of only as an external file.
2. Wire or remove `CursorStore` offline outbox: `queue_offline`/`pending_outbox`/`mark_sent`/`remember_send` are currently dead code — `FileBus`/CLI never call them, so "offline retry" is not demonstrated end-to-end.
3. `bus_cli.py:31` cred file: `write_text` without `0600` (store files are `0600`/`0700`). Set restrictive perms on credential files containing tokens.
4. Make `FileBus.reply` atomic under one `FileLock` hold; add fsync/locking to `CursorStore._save` if it stays.
5. Extend tests: `not_recipient`, `unknown_identity`, `unknown_message` rejection paths (manually verified here, but not in pytest), plus duplicate-delivery/at-least-once consumer handling and offline-outbox replay once (2) lands.

## Residual risk

- Single-node JSON-file store: correct for a local checkpoint, but concurrent multi-writer throughput, torn-write across hosts, clock skew (`created_at` string sort), and token-revocation/rotation are unaddressed — all out of scope for this review, must be covered by the two-host milestone.
- At-least-once only: duplicate `reply` with reused idempotency keys and consumer-side dedup are not exercised; no exactly-once side-effect claim is made (good).
- No transport/auth beyond bearer tokens on a shared filesystem; scoped-auth two-host work is future.

## Statement

Local FileBus direction is sound and honestly scoped; no session forgery, no secret leakage, real-process dogfood. Accept the core as a local checkpoint subject to fixes 1–5; do **not** treat this review as cross-host, deploy, or runtime acceptance.
