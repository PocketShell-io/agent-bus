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
from coordination.durable import write_secret_json


def _bus(args: argparse.Namespace) -> FileBus:
    return FileBus(args.store)


def _cred(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def cmd_register(args: argparse.Namespace) -> int:
    parent_id = None
    parent_token = None
    project_id = args.project
    if args.parent_cred:
        parent = _cred(args.parent_cred)
        parent_id = parent["identity_id"]
        parent_token = parent["token"]
        if not project_id:
            project_id = parent["project_id"]
    ident, token = _bus(args).register(
        agent_name=args.agent,
        device_id=args.device,
        project_id=project_id or "agent-coordination",
        task_id=args.task,
        parent_id=parent_id,
        parent_token=parent_token,
    )
    out = ident.public()
    out["token"] = token
    write_secret_json(Path(args.cred), out)
    print(json.dumps({"identity_id": ident.identity_id, "agent_name": ident.agent_name, "cred": args.cred}))
    return 0


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


def cmd_show(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    msg = _bus(args).get(cred["identity_id"], cred["token"], args.message_id)
    print(json.dumps(msg.to_public(), indent=2))
    return 0


def cmd_ack(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    msg = _bus(args).ack(cred["identity_id"], cred["token"], args.message_id)
    print(json.dumps(msg.to_public(), indent=2))
    return 0


def cmd_accept(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    msg = _bus(args).accept(cred["identity_id"], cred["token"], args.message_id)
    print(json.dumps(msg.to_public(), indent=2))
    return 0


def cmd_complete(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    extra = json.loads(args.extra) if args.extra else None
    msg = _bus(args).complete(
        cred["identity_id"],
        cred["token"],
        args.message_id,
        status=args.status,
        artifact=args.artifact,
        digest=args.digest,
        extra=extra,
    )
    print(json.dumps(msg.to_public(), indent=2))
    return 0


def cmd_reply(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    msg = _bus(args).reply(
        sender_id=cred["identity_id"],
        token=cred["token"],
        message_id=args.message_id,
        body=args.body,
        data=json.loads(args.data) if args.data else None,
        idempotency_key=args.idempotency_key,
    )
    print(json.dumps(msg.to_public(), indent=2))
    return 0


def cmd_queue(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    record = _bus(args).queue_offline(
        sender_id=cred["identity_id"],
        token=cred["token"],
        recipient_id=args.to,
        body=args.body,
        data=json.loads(args.data) if args.data else None,
        idempotency_key=args.idempotency_key,
    )
    print(json.dumps(record, indent=2))
    return 0


def cmd_flush(args: argparse.Namespace) -> int:
    cred = _cred(args.cred)
    items = _bus(args).flush_outbox(cred["identity_id"], cred["token"])
    print(json.dumps([m.to_public() for m in items], indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Agent Bus CLI (no aplexer dependency)")
    parser.add_argument("--store", required=True)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("register")
    p.add_argument("--agent", required=True)
    p.add_argument("--device", required=True)
    p.add_argument("--project", default=None)
    p.add_argument("--task")
    p.add_argument("--cred", required=True)
    p.add_argument("--parent-cred")
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

    p = sub.add_parser("show")
    p.add_argument("--cred", required=True)
    p.add_argument("--message-id", required=True)
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("ack")
    p.add_argument("--cred", required=True)
    p.add_argument("--message-id", required=True)
    p.set_defaults(func=cmd_ack)

    p = sub.add_parser("accept")
    p.add_argument("--cred", required=True)
    p.add_argument("--message-id", required=True)
    p.set_defaults(func=cmd_accept)

    p = sub.add_parser("complete")
    p.add_argument("--cred", required=True)
    p.add_argument("--message-id", required=True)
    p.add_argument("--status", required=True)
    p.add_argument("--artifact")
    p.add_argument("--digest")
    p.add_argument("--extra")
    p.set_defaults(func=cmd_complete)

    p = sub.add_parser("reply")
    p.add_argument("--cred", required=True)
    p.add_argument("--message-id", required=True)
    p.add_argument("--body", required=True)
    p.add_argument("--data")
    p.add_argument("--idempotency-key")
    p.set_defaults(func=cmd_reply)

    p = sub.add_parser("queue")
    p.add_argument("--cred", required=True)
    p.add_argument("--to", required=True)
    p.add_argument("--body", required=True)
    p.add_argument("--data")
    p.add_argument("--idempotency-key")
    p.set_defaults(func=cmd_queue)

    p = sub.add_parser("flush")
    p.add_argument("--cred", required=True)
    p.set_defaults(func=cmd_flush)

    args = parser.parse_args(argv)
    if args.cmd == "register" and not args.project and not args.parent_cred:
        args.project = "agent-coordination"
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
