# tests/integration/test_queue_end_to_end.py
import os
import time
import json
import urllib.parse
import pytest

# ---- HTTP helper: pakai requests jika ada, fallback ke urllib ----
try:
    import requests  # type: ignore
except Exception:
    requests = None
    import urllib.request

    class _Resp:
        def __init__(self, code, data):
            self.status_code = code
            self._data = data
        def json(self):
            return json.loads(self._data.decode("utf-8"))
        @property
        def text(self):
            return self._data.decode("utf-8")

    def _urlopen(method, url, params=None, json_body=None, timeout=5):
        if params:
            q = urllib.parse.urlencode(params, doseq=True)
            url = f"{url}?{q}"
        data = None
        headers = {"Content-Type": "application/json"}
        if json_body is not None:
            data = json.dumps(json_body).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return _Resp(r.status, r.read())

def _get(url, params=None, timeout=5):
    if requests:
        return requests.get(url, params=params, timeout=timeout)
    return _urlopen("GET", url, params=params, timeout=timeout)

def _post(url, params=None, json_body=None, timeout=5):
    if requests:
        return requests.post(url, params=params, json=json_body, timeout=timeout)
    return _urlopen("POST", url, params=params, json_body=json_body, timeout=timeout)

# ---- Config via ENV (default ke localhost) ----
QUEUE_PRIMARY = os.getenv("QUEUE_PRIMARY", "http://localhost:8180")
QUEUE_REPLICA = os.getenv("QUEUE_REPLICA", "http://localhost:8182")
VIS_TTL_MS = int(os.getenv("QUEUE_VIS_TTL_MS", "2000"))

def _service_up(base_url: str) -> bool:
    try:
        r = _get(f"{base_url}/readyz")
        j = r.json() if hasattr(r, "json") else {}
        return (r.status_code in (200, 204)) and (j.get("ready") is True or j.get("status") == "ok")
    except Exception:
        return False

@pytest.mark.integration
def test_queue_end_to_end_publish_consume_ack():
    """
    E2E:
      1) publish N pesan ke primary
      2) consume dari replica (mengembalikan item + field 'owner')
      3) ACK tiap msg_id langsung ke owner sebenarnya (localhost:<port>/queue/ack)
      4) tunggu > TTL, mustinya tidak re-deliver
    """
    if not (_service_up(QUEUE_PRIMARY) and _service_up(QUEUE_REPLICA)):
        pytest.skip("Queue services not up (readyz failed)")

    # topic unik biar tak bentrok antar run
    topic = f"t_integration_{int(time.time()*1000)}"
    key = "user_itg"
    N = 5

    # 1) Publish
    for i in range(1, N + 1):
        r = _post(f"{QUEUE_PRIMARY}/queue/publish",
                  params={"topic": topic, "key": key},
                  json_body={"i": i})
        assert r.status_code == 200, f"publish failed: {r.text}"

    # 2) Consume dari replica
    r = _post(f"{QUEUE_REPLICA}/queue/consume",
              params={"topic": topic, "key": key, "visibility_ttl": VIS_TTL_MS, "max": N})
    assert r.status_code == 200, f"consume failed: {r.text}"
    items = r.json()
    assert isinstance(items, list) and len(items) == N, f"expected {N} items, got {len(items)}"

    # 3) ACK: langsung ke owner (tanpa /ack_owner), idempotent + retry kecil
    from urllib.parse import urlparse
    for it in items:
        owner = it["owner"]          # contoh: http://queue3:8182
        msg_id = it["msg_id"]
        p = urlparse(owner)
        port = p.port or (443 if p.scheme == "https" else 80)
        owner_local = f"http://localhost:{port}"

        acked = False
        for _ in range(3):  # kecil-kecilan antisipasi race timing
            r2 = _post(f"{owner_local}/queue/ack", params={"topic": topic, "msg_id": msg_id})
            if r2.status_code == 200:
                j2 = r2.json()
                if j2.get("acked") is True or j2.get("reason") == "already_acked":
                    acked = True
                    break
            time.sleep(0.05)
        assert acked, f"ack failed at owner {owner} for msg {msg_id}"

    # 4) Pastikan tidak re-deliver setelah > TTL
    time.sleep((VIS_TTL_MS / 1000.0) + 0.5)
    r = _post(f"{QUEUE_REPLICA}/queue/consume",
              params={"topic": topic, "key": key, "visibility_ttl": VIS_TTL_MS, "max": N})
    assert r.status_code == 200
    assert r.json() == [], "should be empty after ACK+TTL"
