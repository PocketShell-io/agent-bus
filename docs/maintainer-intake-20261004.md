# Maintainer intake — 4 October 2026

Source: direct human messages to the saved Codex principal conversation, immediately following capacity-recovery work. This is recorded as product intake, not a completed implementation.

## Requested behavior

- Extract/decouple aplexer message-bus functionality into a separate sibling project, named Agent Bus for now, under the PocketShell organization (verified GitHub organization PocketShell-io).
- Plain synchronous/headless agents communicate without their own aplexer session. Keep useful visible aplexer sessions for heads and agents that supervise others.
- Independent bus identity, send/receive/wait/reply/read ACK and durable message IDs form the cross-computer/multi-machine foundation. Never impersonate an aplexer session to obtain this.
- Start using the first viable version immediately in real development. Preserve fallback and recovery. When a workaround or repetitive operation appears, implement a native product feature and record it as maintainer intake.
- Preserve current four-product teams and work; existing coordination head integrates this bus foundation, not a duplicate principal/team.

## First acceptance milestone

A independently reviewed local implementation runs two plain headless processes with no aplexer session/PID/env/executable dependency, assigns genuine distinct bus identities, exchanges an actual owned development task and its artifact, and records delivery/read ACK/acceptance/outcome separately. Demonstrate persistence, process restart, duplicate delivery/idempotent consumer handling, wrong-identity rejection and offline retry. At-least-once delivery is acceptable; do not promise exactly-once side effects. Then exercise two real computers using scoped existing authentication, distinguish host bridge from originating agent and preserve draft/busy protection in optional UI adapters.

## Intake and ownership

Existing head81e801 assigned; ACK pending. Head decomposes core/storage/identity CLI, transport adapter and independent review; scopes and actual executors recorded before edits. Principal bootstrap owns only README.md, AGENTS.md, .gitignore and this intake until released. Implementation paths free for acknowledged delegation; no peer source copied without handoff.

Related repeated-operation intake: /home/alexey/git/cloudflare-agent-git/research/codex/aplexer-repeated-operations.md. Capacity policy is a separate uninstalled candidate, not automatic bus/session recovery.

Latest visibility correction: human explicitly requires PUBLIC GitHub repository; overrides default-private bootstrap assumption. Public source only; private logs/tokens/config stay excluded.
Latest session lifecycle intake: audit idle sessions, stop unneeded completed/abandoned executors while preserving history, and document an actual reason for every retained idle session. Idle is not sufficient to infer safe termination; checkpoint owned work and pending tasks. Heads remain visible; ordinary executor tasks can start fresh as needed.
