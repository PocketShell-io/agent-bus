import hashlib
import json
import os
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

def run_cmd(cmd):
    subprocess.run(cmd, shell=True, check=True)

def main():
    store_dir = tempfile.mkdtemp()
    certs_dir = tempfile.mkdtemp()
    
    # Generate certs
    run_cmd(f"bash tools/make-certs.sh {certs_dir}")
    
    with open(f"{certs_dir}/client.crt", "rb") as f:
        cert_data = f.read()
    
    output = subprocess.check_output(f"openssl x509 -in {certs_dir}/client.crt -outform DER", shell=True)
    fingerprint = hashlib.sha256(output).hexdigest()

    allowlist = {
        fingerprint: {
            "devices": ["device-01"],
            "projects": ["agent-bus-project"]
        }
    }
    
    allowlist_path = os.path.join(store_dir, "allowlist.json")
    with open(allowlist_path, "w") as f:
        json.dump(allowlist, f)
        
    port = 18788
    server_cmd = [
        "python3", "adapter/adapter.py",
        "--port", str(port),
        "--store", store_dir,
        "--allowlist", allowlist_path,
        "--ca-cert", f"{certs_dir}/ca.crt",
        "--server-cert", f"{certs_dir}/server.crt",
        "--server-key", f"{certs_dir}/server.key"
    ]
    
    server_proc = subprocess.Popen(server_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(2)
    
    # Check if server is still running
    if server_proc.poll() is not None:
        print(f"Server crashed: {server_proc.communicate()}")
        sys.exit(1)
        
    ctx_good = ssl.create_default_context(cafile=f"{certs_dir}/ca.crt")
    ctx_good.check_hostname = False
    ctx_good.load_cert_chain(certfile=f"{certs_dir}/client.crt", keyfile=f"{certs_dir}/client.key")
    
    ctx_bad = ssl.create_default_context(cafile=f"{certs_dir}/ca.crt")
    ctx_bad.check_hostname = False
    ctx_bad.load_cert_chain(certfile=f"{certs_dir}/client2.crt", keyfile=f"{certs_dir}/client2.key")
    
    ctx_no_cert = ssl.create_default_context(cafile=f"{certs_dir}/ca.crt")
    ctx_no_cert.check_hostname = False
    
    base_url = f"https://127.0.0.1:{port}/v1"
    
    def do_req(url, method="GET", payload=None, ctx=ctx_good):
        req = urllib.request.Request(url, method=method)
        if payload:
            data = json.dumps(payload).encode("utf-8")
            req.add_header("Content-Type", "application/json")
            req.add_header("Content-Length", len(data))
            req.data = data
        try:
            with urllib.request.urlopen(req, context=ctx) as f:
                return f.status, json.loads(f.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())
        except Exception as e:
            return 0, str(e)
            
    print("Testing negative: no cert")
    status, res = do_req(f"{base_url}/inbox", ctx=ctx_no_cert)
    print(f"no cert result: {status} {res}")
    # The server might drop the connection on handshake failure (no cert presented)
    # So we accept status == 0 or status == 403
    assert status in [0, 403], f"Expected fail no-cert, got {status}"
    
    print("Testing negative: unauthorized cert")
    status, res = do_req(f"{base_url}/inbox?identity_id=foo&token=bar", ctx=ctx_bad)
    print(f"unauthorized cert result: {status} {res}")
    assert status == 403, f"Expected 403 bad cert, got {status}"
    
    print("Testing negative: out-of-scope project")
    status, res = do_req(f"{base_url}/register", method="POST", payload={
        "device_id": "device-01",
        "agent_name": "agent",
        "project_id": "wrong-project"
    }, ctx=ctx_good)
    assert status == 403, f"Expected 403 wrong project, got {status}"
    
    print("Testing positive: register")
    status, res = do_req(f"{base_url}/register", method="POST", payload={
        "device_id": "device-01",
        "agent_name": "agent",
        "project_id": "agent-bus-project"
    }, ctx=ctx_good)
    assert status == 201, f"Expected 201 register, got {status} {res}"
    token = res["token"]
    identity_id = res["identity"]["identity_id"]
    
    print("Testing positive: send message")
    status, res = do_req(f"{base_url}/send", method="POST", payload={
        "sender_id": identity_id,
        "token": token,
                "recipient_id": identity_id,
        "body": "hello",
        "idempotency_key": "key-123"
    }, ctx=ctx_good)
    assert status == 200, f"Expected 200 send, got {status} {res}"
    msg_id = res["message"]["id"]
    
    print("Testing negative: replay idempotency_key")
    status, res = do_req(f"{base_url}/send", method="POST", payload={
        "sender_id": identity_id,
        "token": token,
                "recipient_id": identity_id,
        "body": "hello2",
        "idempotency_key": "key-123"
    }, ctx=ctx_good)
    assert status == 409, f"Expected 409 replay, got {status} {res}"
    
    print("Testing positive: poll inbox")
    status, res = do_req(f"{base_url}/inbox?identity_id={identity_id}&token={token}", ctx=ctx_good)
    assert status == 200, f"Expected 200 inbox, got {status} {res}"

    messages = res["messages"]
    assert len(messages) == 1
    poll_msg_id = messages[0]["message_id"]
    
    print("Testing positive: ack")
    status, res = do_req(f"{base_url}/ack", method="POST", payload={
        "identity_id": identity_id,
        "token": token,
        "message_id": poll_msg_id
    }, ctx=ctx_good)
    assert status == 200, f"Expected 200 ack, got {status} {res}"

    print("Testing positive: accept")
    status, res = do_req(f"{base_url}/accept", method="POST", payload={
        "identity_id": identity_id,
        "token": token,
        "message_id": poll_msg_id
    }, ctx=ctx_good)
    assert status == 200, f"Expected 200 accept, got {status} {res}"

    print("Testing positive: complete")
    status, res = do_req(f"{base_url}/complete", method="POST", payload={
        "identity_id": identity_id,
        "token": token,
        "message_id": poll_msg_id,
        "status": "completed"
    }, ctx=ctx_good)
    assert status == 200, f"Expected 200 complete, got {status} {res}"

    
    # Clean shutdown
    server_proc.kill()
    print("All tests passed.")

if __name__ == "__main__":
    main()
