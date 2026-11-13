"""Push delivery's receiving end: the broker POSTs a served task here instead of the
bridge long-polling for it (the "Push delivery for cloud agents" spec, broker side in
ag2space-backend services/agent-message-broker/broker/push.py).

On only when the pod has AG2_PUSH_SECRET and AG2_PUSH_PORT (core-api sets both for an AG2
Assistant instance with SUTANDO_PUSH_DELIVERY on). Then remote_gateway_bridge's loop
takes its tasks from here instead of GET /v1/tasks, and runs its own round on them
unchanged: the same dedupe, the same durable write to tasks/, the same ack. The POST is
answered only after that round settled the task:

  2xx  {"id", "durable": true}   the round acked, staged or consumed it (it is on disk,
                                 or it was a control the bridge consumed)
  503                            anything else (dropped, unsettled, the loop busy past
                                 the wait): the broker leaves the lease to run out and
                                 serves the task again; the bridge's dedupe absorbs it
  401                            bad or stale signature; nothing is queued

Signature: X-AG2-Push-Signature = "v1=" + hex HMAC-SHA256(secret, "<ts>." + body), with
<ts> (unix seconds) in X-AG2-Push-Timestamp and within MAX_SKEW_S of now.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SIG_HEADER = "X-AG2-Push-Signature"
TS_HEADER = "X-AG2-Push-Timestamp"
MAX_SKEW_S = 300
MAX_BODY = 8 * 1024 * 1024
ANSWER_WAIT_S = 30.0          # the broker's POST times out at 10 s; a slot never outlives this
DELIVERED = ("acked", "staged", "consumed")


def sign(secret: str, body: bytes, ts: str) -> str:
    mac = hmac.new(secret.encode(), ts.encode() + b"." + body, hashlib.sha256)
    return "v1=" + mac.hexdigest()


def verified(secret: str, body: bytes, ts: str, got: str, now: float) -> bool:
    try:
        fresh = abs(now - int(ts)) <= MAX_SKEW_S
    except (TypeError, ValueError):
        return False
    return fresh and hmac.compare_digest(sign(secret, body, ts), got or "")


class _Slot:
    __slots__ = ("task", "done", "fate")

    def __init__(self, task: dict):
        self.task = task
        self.done = threading.Event()
        self.fate: "str | None" = None


class PushInbox:
    """POSTed tasks waiting for the bridge loop's next round, and their answers."""

    def __init__(self, secret: str, port: int, key, *, host: str = "0.0.0.0",
                 clock=time.time, log=print):
        self.secret = secret
        self.port = port
        self._key = key                    # the bridge's _served_key: how a round names a task
        self._clock = clock
        self._log = log
        self._q: "queue.Queue[_Slot]" = queue.Queue()
        self._waiting: dict[str, list] = {}
        self._lock = threading.Lock()
        self._host = host
        self._server: "ThreadingHTTPServer | None" = None

    @classmethod
    def from_env(cls, key, env=None, **kw) -> "PushInbox | None":
        e = os.environ if env is None else env
        secret, port = e.get("AG2_PUSH_SECRET") or "", e.get("AG2_PUSH_PORT") or ""
        if not secret or not port.isdigit():
            return None
        return cls(secret, int(port), key, **kw)

    # -- the HTTP side ---------------------------------------------------------------

    def start(self) -> None:
        inbox = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 — http.server's name
                if self.path.rstrip("/") != "/v1/messages":
                    return self._answer(404, {"error": "not found"})
                n = int(self.headers.get("Content-Length") or 0)
                if n <= 0 or n > MAX_BODY:
                    return self._answer(413 if n > MAX_BODY else 400, {"error": "body"})
                body = self.rfile.read(n)
                code, out = inbox.receive(body, self.headers.get(TS_HEADER, ""),
                                          self.headers.get(SIG_HEADER, ""))
                self._answer(code, out)

            def _answer(self, code, payload):
                raw = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *a):  # the bridge logs what matters itself
                pass

        self._server = ThreadingHTTPServer((self._host, self.port), Handler)
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]
        threading.Thread(target=self._server.serve_forever, name="push-inbox",
                         daemon=True).start()
        self._log(f"push inbox listening on :{self.port} — no long-poll")

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

    def receive(self, body: bytes, ts: str, sig: str, *,
                wait_s: float = ANSWER_WAIT_S) -> "tuple[int, dict]":
        """Verify, queue for the loop, and wait for the round's fate."""
        if not verified(self.secret, body, ts, sig, self._clock()):
            return 401, {"error": "bad signature"}
        try:
            task = json.loads(body)
        except ValueError:
            return 400, {"error": "not json"}
        if not isinstance(task, dict) or not task.get("id"):
            return 400, {"error": "not a task"}
        slot = _Slot(task)
        with self._lock:
            self._waiting.setdefault(self._key(task), []).append(slot)
        self._q.put(slot)
        if not slot.done.wait(wait_s):
            with self._lock:
                left = self._waiting.get(self._key(task), [])
                if slot in left:
                    left.remove(slot)
            return 503, {"id": task["id"], "error": "not settled in time"}
        if slot.fate in DELIVERED:
            return 200, {"id": task["id"], "durable": True}
        return 503, {"id": task["id"], "error": f"not taken ({slot.fate or 'unsettled'})"}

    # -- the loop side ---------------------------------------------------------------

    def take(self, timeout: float) -> list:
        """The next round's tasks: waits up to `timeout` for the first, then takes what
        else is already queued. Stands in for the long-poll's answer."""
        try:
            first = self._q.get(timeout=max(0.0, timeout))
        except queue.Empty:
            return []
        slots = [first]
        while True:
            try:
                slots.append(self._q.get_nowait())
            except queue.Empty:
                break
        return [s.task for s in slots]

    def settle(self, fates: dict) -> None:
        """After the round: answer every POST whose task the round named."""
        with self._lock:
            for key, fate in fates.items():
                for slot in self._waiting.pop(key, []):
                    slot.fate = fate
                    slot.done.set()

    def fail_round(self, tasks: list) -> None:
        """A round that raised: its tasks' POSTs are answered 503 now, not at the timeout."""
        self.settle({self._key(t): None for t in tasks if isinstance(t, dict)})
