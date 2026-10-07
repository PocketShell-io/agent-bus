import argparse
import hashlib
import json
import logging
import pathlib
import ssl
import sys
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

# Add parent dir to path to find bus_core
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
try:
    from coordination.bus import FileBus
    from coordination.errors import BusError, CoordinationError
except ImportError:
    # Fallback to standard generic classes if bus_core doesn't exist in the same way
    class BusError(Exception): pass
    class CoordinationError(Exception): pass

logger = logging.getLogger(__name__)

class BusAdapterHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: HTTPStatus, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _get_cert_fingerprint(self):
        if not hasattr(self.server, "ssl_context") or self.server.ssl_context is None:
            return "insecure_mode"
        cert_der = self.connection.getpeercert(binary_form=True)
        if not cert_der:
            return None
        return hashlib.sha256(cert_der).hexdigest()

    def _verify_allowlist(self, payload: dict) -> bool:
        fingerprint = self._get_cert_fingerprint()
        if fingerprint == "insecure_mode":
            return True
        if not fingerprint:
            self._send_json(HTTPStatus.FORBIDDEN, {"status": "error", "detail": "Client cert required"})
            return False

        allowlist = self.server.allowlist
        if fingerprint not in allowlist:
            self._send_json(HTTPStatus.FORBIDDEN, {"status": "error", "detail": "Cert fingerprint not allowed"})
            return False
            
        allowed = allowlist[fingerprint]
        
        # Check scopes
        device_id = payload.get("device_id")
        project_id = payload.get("project_id")
        
        # We need to map depending on payload, but basically:
        if device_id and device_id not in allowed.get("devices", []):
            self._send_json(HTTPStatus.FORBIDDEN, {"status": "error", "detail": f"Device scope {device_id} not allowed"})
            return False
            
        if project_id and project_id not in allowed.get("projects", []):
            self._send_json(HTTPStatus.FORBIDDEN, {"status": "error", "detail": f"Project scope {project_id} not allowed"})
            return False
            
        return True

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path

            if path == "/v1/inbox":
                query = parse_qs(parsed.query)
                identity_id = query.get("identity_id", [None])[0]
                token = query.get("token", [None])[0]
                unread_only = query.get("unread_only", ["false"])[0].lower() == "true"
                cursor = query.get("cursor", [None])[0]
                
                if not identity_id or not token:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"status": "error", "detail": "identity_id and token required"})
                    return
                    
                if not self._verify_allowlist({}):
                    return

                messages = self.server.bus.inbox(identity_id=identity_id,
                    token=token,  unread_only=unread_only)
                self._send_json(HTTPStatus.OK, {
                    "status": "ok",
                    "messages": [m.to_public() for m in messages],
                })
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"status": "error", "detail": "Path not found"})
        except BusError as err:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "error", "detail": str(err)})
        except Exception as exc:
            logger.exception("Internal error")
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"status": "error", "detail": str(exc)})

    def do_POST(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path

            content_length = int(self.headers.get("Content-Length", 0))
            if content_length == 0:
                payload = {}
            else:
                body = self.rfile.read(content_length)
                payload = json.loads(body)
                
            if not self._verify_allowlist(payload):
                return

            if path == "/v1/register":
                device_id = payload.get("device_id")
                agent_name = payload.get("agent_name")
                project_id = payload.get("project_id")

                if not device_id or not agent_name or not project_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"status": "error", "detail": "Missing fields"})
                    return

                ident_obj, token = self.server.bus.register(
                    device_id=device_id,
                    agent_name=agent_name,
                    project_id=project_id
                )
                ident_data = getattr(ident_obj, '__dict__', ident_obj)
                self._send_json(HTTPStatus.CREATED, {
                    "status": "ok",
                    "identity": ident_data,
                    "token": token,
                })
                return

            if path == "/v1/send":
                sender_id = payload.get("sender_id")
                token = payload.get("token")
                recipient_id = payload.get("recipient_id")
                body = payload.get("body")
                idempotency_key = payload.get("idempotency_key")
                
                if idempotency_key:
                    if idempotency_key in self.server.seen_idempotency_keys:
                        self._send_json(HTTPStatus.CONFLICT, {"status": "error", "detail": "Replay detected"})
                        return
                    self.server.seen_idempotency_keys.add(idempotency_key)

                if not sender_id or not token or not recipient_id or body is None:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"status": "error", "detail": "Missing fields"})
                    return

                msg = self.server.bus.send(
                    sender_id=sender_id,
                    token=token,
                    
                    recipient_id=recipient_id,
                    body=body,
                    idempotency_key=idempotency_key,
                )
                msg_data = msg.to_public()
                msg_data["id"] = msg.message_id
                self._send_json(HTTPStatus.OK, {"status": "ok", "message": msg_data})
                return

            for action in ["ack", "accept"]:
                if path == f"/v1/{action}":
                    identity_id = payload.get("identity_id")
                    token = payload.get("token")
                    message_id = payload.get("message_id")
                    if not identity_id or not token or not message_id:
                        self._send_json(HTTPStatus.BAD_REQUEST, {"status": "error", "detail": "Missing fields"})
                        return
                    fn = getattr(self.server.bus, action)
                    msg = fn(identity_id=identity_id,
                    token=token,  message_id=message_id)
                    self._send_json(HTTPStatus.OK, {"status": "ok", "message": msg.to_public()})
                    return

            if path == "/v1/complete":
                identity_id = payload.get("identity_id")
                token = payload.get("token")
                message_id = payload.get("message_id")
                status = payload.get("status", "completed")
                
                if not identity_id or not token or not message_id:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"status": "error", "detail": "Missing fields"})
                    return
                msg = self.server.bus.complete(
                    identity_id=identity_id,
                    token=token,
                    
                    message_id=message_id,
                    status=status,
                )
                self._send_json(HTTPStatus.OK, {"status": "ok", "message": msg.to_public()})
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"status": "error", "detail": "Path not found"})
        except BusError as err:
            self._send_json(HTTPStatus.BAD_REQUEST, {"status": "error", "detail": str(err)})
        except Exception as exc:
            logger.exception("Internal error")
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"status": "error", "detail": str(exc)})

class BusServer(HTTPServer):
    def __init__(self, server_address, handler_class, bus, allowlist, ssl_context=None):
        super().__init__(server_address, handler_class)
        self.bus = bus
        self.allowlist = allowlist
        self.ssl_context = ssl_context
        self.seen_idempotency_keys = set()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8788)
    parser.add_argument("--store", type=pathlib.Path, required=True)
    parser.add_argument("--allowlist", type=pathlib.Path)
    parser.add_argument("--ca-cert", type=pathlib.Path)
    parser.add_argument("--server-cert", type=pathlib.Path)
    parser.add_argument("--server-key", type=pathlib.Path)
    parser.add_argument("--insecure-loopback", action="store_true")
    args = parser.parse_args()

    # Dynamic import to ensure it works
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
    from coordination.bus import FileBus
    bus = FileBus(args.store)
    
    allowlist = {}
    if args.allowlist and args.allowlist.exists():
        with open(args.allowlist) as f:
            allowlist = json.load(f)

    ctx = None
    if not args.insecure_loopback:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=args.server_cert, keyfile=args.server_key)
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=args.ca_cert)
        
    server = BusServer((args.host, args.port), BusAdapterHandler, bus, allowlist, ctx)
    if ctx:
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        
    logger.info("Starting adapter...")
    server.serve_forever()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
