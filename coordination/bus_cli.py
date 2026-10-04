#!/usr/bin/env python3
"""Headless bus CLI. No aplexer executable, PID, or env required."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from coordination.bus import FileBus


def _bus(args: argparse.Namespace) -> FileBus:
    return FileBus(args.store)


def cmd_register(args: argparse.Namespace) -> int:
    ident, token = _bus(args).register(
        agent_name=args.agent,
        device_id=args.device,
        project_id=args.project,
        task_id=args.task,
    )
    out = ident.public()
    out["token"] = token
    Path(args.cred).write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps({"identity_id": ident.identity_id, "agent_name": ident.agent_name, "cred": args.cred}))
    return 0


def _cred(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def cmd_send(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    msg = _bus(args).send(
        sender_id=cred["identity_id"],
        token=cred["token"],
        recipient_id=args.to,
        body=args.body,
        data=json.loads(args.data) if args.data else None,
        idempotency_key=args.idempotency_key,
    )
    print(json.dumps(msg.to_public(), indent=2))
    return 0


def cmd_inbox(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    items = _bus(args).inbox(cred["identity_id"], cred["token"], unread_only=not args.all)
    print(json.dumps([m.to_public() for m in items], indent=2))
    return 0


def cmd_wait(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    items = _bus(args).wait(cred["identity_id"], cred["token"], timeout=args.timeout)
    print(json.dumps([m.to_public() for m in items], indent=2))
    return 0 if items else 2


def cmd_ack(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    msg = _bus(args).ack(cred["identity_id"], cred["token"], args.message_id)
    print(json.dumps(msg.to_public(), indent=2))
    return 0


def cmd_reply(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    msg = _bus(args).reply(
        sender_id=cred["identity_id"],
        token=cred["token"],
        message_id=args.message_id,
        body=args.body,
        idempotency_key=args.idempotency_key,
    )
    print(json.dumps(msg.to_public(), indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Agent Bus CLI (no aplexer dependency)")
    parser.add_argument("--store", required=True)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("register")
    p.add_argument("--agent", required=True)
    p.add_argument("--device", required=True)
    p.add_argument("--project", default="agent-coordination")
    p.add_argument("--task")
    p.add_argument("--cred", required=True)
    p.set_defaults(func=cmd_register)

    p = sub.add_parser("send")
    p.add_argument("--cred", required=True)
    p.add_argument("--to", required=True)
    p.add_argument("--body", required=True)
    p.add_argument("--data")
    p.add_argument("--idempotency-key")
    p.set_defaults(func=cmd_send)

    p = sub.add_parser("inbox")
    p.add_argument("--cred", required=True)
    p.add_argument("--all", action="store_true")
    p.set_defaults(func=cmd_inbox)

    p = sub.add_parser("wait")
    p.add_argument("--cred", required=True)
    p.add_argument("--timeout", type=float, default=5.0)
    p.set_defaults(func=cmd_wait)

    p = sub.add_parser("ack")
    p.add_argument("--cred", required=True)
    p.add_argument("--message-id", required=True)
    p.set_defaults(func=cmd_ack)

    p = sub.add_parser("reply")
    p.add_argument("--cred", required=True)
    p.add_argument("--message-id", required=True)
    p.add_argument("--body", required=True)
    p.add_argument("--idempotency-key")
    p.set_defaults(func=cmd_reply)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
