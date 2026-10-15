> **Deprecated: superseded by [Agent Hub](https://github.com/alexeygrigorev/agent-hub).**
> Use Agent Hub for the authenticated cross-host message API and host bridges. Preserve durable message IDs and the separate receipt, ACK, acceptance and completion stages. Retire an existing adapter only after replacement traffic is verified. See the [migration guide](https://github.com/alexeygrigorev/agent-hub/blob/main/docs/GENERIC-HUB.md).

# Agent Bus

Standalone durable messaging for interactive and headless agents, independent of aplexer terminal sessions. Foundation for cross-machine coordination. Implementation is starting; no working product or cross-host acceptance is claimed yet.

Agents register an independent bus identity and send, receive, reply and acknowledge messages without an aplexer process, session, or inherited APLEXER environment. Aplexer can consume the bus as a UI/transport adapter. Delivery receipt, read ACK, semantic acceptance and actual task outcome stay separate.

Source: public PocketShell-io/agent-bus. Sibling workspace: /home/alexey/git/agent-bus. Existing agent-coordination source and histories are preserved.

Integration owner: existing agent-coordination-head (81e8010c-89e4-478b-be3a-4ee6991607f3), ownership acceptance pending. Principal monitor: codex-principal93cf28f2.

See [maintainer intake](docs/maintainer-intake-20261004.md). First milestone: independently reviewed local CLI/API, durable restart/idempotency tests, then a real task and artifact exchanged by two plain headless processes with no aplexer sessions. Two-host validation follows.
