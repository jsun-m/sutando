"""Push delivery's receiving end (push_inbox.py): a signed POST is queued for the bridge
loop's next round and answered with that round's fate, 2xx only once the task is
durably taken. Over real HTTP on a loopback port; the round is the bridge's own."""
import importlib
import json
import os
import pathlib
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

sys.path.append(str(pathlib.Path(__file__).resolve().parents[3] / "tests"))
from _helpers.hermetic_env import isolate_os_environ  # noqa: E402

isolate_os_environ()
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ag2_sparrow import push_inbox  # noqa: E402

SECRET = "push-secret"


def _key(task):
    return str(task.get("id"))


def _inbox(**kw):
    box = push_inbox.PushInbox(SECRET, 0, _key, host="127.0.0.1", log=lambda *_: None, **kw)
    box.start()
    return box


def _post(box, task, *, secret=SECRET, ts=None, timeout=10):
    body = json.dumps(task).encode()
    ts = ts or str(int(time.time()))
    req = urllib.request.Request(
        f"http://127.0.0.1:{box.port}/v1/messages", data=body, method="POST",
        headers={"Content-Type": "application/json", push_inbox.TS_HEADER: ts,
                 push_inbox.SIG_HEADER: push_inbox.sign(secret, body, ts)})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def _post_async(box, task, **kw):
    out = {}
    t = threading.Thread(target=lambda: out.update(answer=_post(box, task, **kw)))
    t.start()
    return t, out


def test_off_unless_both_the_secret_and_the_port_are_set():
    assert push_inbox.PushInbox.from_env(_key, env={}) is None
    assert push_inbox.PushInbox.from_env(_key, env={"AG2_PUSH_SECRET": "s"}) is None
    assert push_inbox.PushInbox.from_env(_key, env={"AG2_PUSH_PORT": "8095"}) is None
    box = push_inbox.PushInbox.from_env(_key, env={"AG2_PUSH_SECRET": "s",
                                                   "AG2_PUSH_PORT": "8095"})
    assert box is not None and box.port == 8095


def test_a_bad_or_stale_signature_is_refused_and_nothing_is_queued():
    box = _inbox()
    try:
        assert _post(box, {"id": "t1"}, secret="wrong")[0] == 401
        stale = str(int(time.time()) - push_inbox.MAX_SKEW_S - 60)
        assert _post(box, {"id": "t1"}, ts=stale)[0] == 401
        assert box.take(0.1) == []
    finally:
        box.stop()


def test_the_post_is_answered_with_the_rounds_fate():
    box = _inbox()
    try:
        a, out_a = _post_async(box, {"id": "t1", "task": "x"})
        b, out_b = _post_async(box, {"id": "t2", "task": "y"})
        got = []
        deadline = time.time() + 5
        while len(got) < 2 and time.time() < deadline:
            got += box.take(1.0)
        assert sorted(t["id"] for t in got) == ["t1", "t2"]
        box.settle({"t1": "acked", "t2": "dropped"})
        a.join(5)
        b.join(5)
        assert out_a["answer"] == (200, {"id": "t1", "durable": True})
        assert out_b["answer"][0] == 503 and "dropped" in out_b["answer"][1]["error"]
    finally:
        box.stop()


def test_an_unsettled_post_is_a_503_so_the_broker_serves_it_again():
    box = _inbox()
    try:
        assert box.receive(json.dumps({"id": "t9"}).encode(), str(int(time.time())),
                           push_inbox.sign(SECRET, json.dumps({"id": "t9"}).encode(),
                                           str(int(time.time()))), wait_s=0.2)[0] == 503
    finally:
        box.stop()


def test_a_pushed_task_goes_through_the_bridges_own_round_onto_disk():
    """The loop's round, unchanged, on a task taken from the inbox: written to tasks/,
    acked durably to the broker, and the POST answered 2xx only after that."""
    with tempfile.TemporaryDirectory() as d:
        base = pathlib.Path(d)
        os.environ["AGENT_CONNECT_TASK_DIR"] = str(base / "tasks")
        os.environ["AGENT_CONNECT_RESULT_DIR"] = str(base / "results")
        os.environ["AGENT_CONNECT_STATE_DIR"] = str(base / "state")
        os.environ.setdefault("REMOTE_TASK_URL", "https://gw.example/relay")
        os.environ.setdefault("REMOTE_TASK_TOKEN", "dummy-secret")
        m = importlib.reload(importlib.import_module("ag2_sparrow.remote_gateway_bridge"))
        acks = []
        m._req = lambda method, path, body=None, **k: acks.append((path, body)) or {"ok": True}
        box = push_inbox.PushInbox(SECRET, 0, m._served_key, host="127.0.0.1",
                                   log=lambda *_: None)
        box.start()
        try:
            task = {"id": "task-push-1", "task": "hello from push", "lease_id": "lease-1",
                    "channel_id": "!r:x", "user_id": "@o:x", "event_id": "$e"}
            t, out = _post_async(box, task)
            tasks, fates = m._receive_round(m._poll_task_list({"tasks": box.take(5.0)}))
            m._take_served_round(tasks, set(), [], fates, [])
            m._settle_round(fates)
            box.settle(fates)
            t.join(5)
            assert out["answer"] == (200, {"id": "task-push-1", "durable": True})
            assert any(p.endswith("/v1/tasks/task-push-1/ack") for p, _ in acks)
            assert list((base / "tasks").glob("*task-push-1*"))
        finally:
            box.stop()
