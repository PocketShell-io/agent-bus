# Checkpoint 2026-10-04T11:56Z

Scoped copy from `/home/alexey/git/agent-coordination` (not a history move).
Integration owner: `agent-coordination-head` `81e8010c-89e4-478b-be3a-4ee6991607f3`.

Landed:

- `coordination/bus.py` FileBus register/send/inbox/wait/reply/ACK, fsync, flock
- `coordination/bus_cli.py` headless CLI, no aplexer executable/PID/env
- `tests/test_bus.py` and `tests/test_bus_dogfood.py` (two processes, restart redelivery, real artifact)

pytest 6/6 in this checkout. This is a local stdlib checkpoint, not
cross-host acceptance.
