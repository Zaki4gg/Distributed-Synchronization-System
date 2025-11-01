# tests/performance/test_cache_perf.py
import os, time, json, uuid, pytest

# ---------- HTTP helpers (requests jika ada, fallback urllib) ----------
try:
    import requests  # type: ignore
except Exception:
    requests = None
    import urllib.request, urllib.parse
    class _Resp:
        def __init__(self, code, data): self.status_code=code; self._d=data
        def json(self): return json.loads(self._d.decode("utf-8"))
        @property
        def text(self): return self._d.decode("utf-8")
    def _urlopen(method, url, params=None, json_body=None, timeout=5):
        if params:
            from urllib.parse import urlencode; url=f"{url}?{urlencode(params, doseq=True)}"
        data=None; headers={"Content-Type":"application/json"}
        if json_body is not None: data=json.dumps(json_body).encode()
        req=urllib.request.Request(url, data=data, headers=headers, method=method.upper())
        with urllib.request.urlopen(req, timeout=timeout) as r: return _Resp(r.status, r.read())

def _get(url, params=None, timeout=5):
    if requests: return requests.get(url, params=params, timeout=timeout)
    return _urlopen("GET", url, params=params, timeout=timeout)

def _post(url, params=None, json_body=None, timeout=5):
    if requests: return requests.post(url, params=params, json=json_body, timeout=timeout)
    return _urlopen("POST", url, params=params, json_body=json_body, timeout=timeout)

# ---------- Targets ----------
CACHE1 = os.getenv("CACHE1", "http://localhost:8280")
CACHE2 = os.getenv("CACHE2", "http://localhost:8281")

def _service_up(base: str) -> bool:
    try:
        r = _get(f"{base}/readyz")
        return r.status_code == 200 and (r.json() or {}).get("ready") is True
    except Exception:
        return False

@pytest.mark.performance
def test_cache_put_get_throughput_and_coherence():
    """
    Micro-benchmark + cek koherensi:
      - 200 PUT ke cache1
      - 1000 GET hot-key ke cache1 (ekspektasi hit)
      - PUT update di cache1, baca dari cache2 sampai koheren (poll ≤ 2s)
    Threshold throughput dibuat sangat longgar agar stabil lintas mesin.
    """
    if not (_service_up(CACHE1) and _service_up(CACHE2)):
        pytest.skip("Cache services not up (readyz failed)")

    # --- 200 PUT ke cache1 ---
    N_PUT = int(os.getenv("PERF_PUT_N", "200"))
    t0 = time.perf_counter()
    for i in range(N_PUT):
        r = _post(f"{CACHE1}/cache/put", json_body={"key": f"p_{uuid.uuid4().hex[:8]}_{i}", "value": i})
        assert r.status_code == 200 and r.json().get("stored") is True
    t1 = time.perf_counter()
    put_qps = N_PUT / max(1e-6, (t1 - t0))

    # --- 1000 GET hot-key ke cache1 ---
    hot_key = f"hot_{uuid.uuid4().hex[:8]}"
    _post(f"{CACHE1}/cache/put", json_body={"key": hot_key, "value": 123})
    N_GET = int(os.getenv("PERF_GET_N", "1000"))
    t2 = time.perf_counter()
    for _ in range(N_GET):
        r = _get(f"{CACHE1}/cache/get", params={"key": hot_key})
        assert r.status_code == 200
        j = r.json()
        assert j.get("hit") is True and j.get("value") == 123
    t3 = time.perf_counter()
    get_qps = N_GET / max(1e-6, (t3 - t2))

    # --- Koherensi: update di cache1, baca dari cache2 ---
    coh_key = f"coh_{uuid.uuid4().hex[:8]}"
    _post(f"{CACHE1}/cache/put", json_body={"key": coh_key, "value": "v1"})

    # Poll sampai MESI invalidation/propagasi selesai (≤ 2s)
    ok = False; deadline = time.time() + 2.0
    while time.time() < deadline:
        r = _get(f"{CACHE2}/cache/get", params={"key": coh_key})
        if r.status_code == 200 and r.json().get("value") == "v1":
            ok = True; break
        time.sleep(0.05)
    assert ok, "coherence check failed: cache2 tidak melihat 'v1' dalam waktu wajar"

    # --- Threshold sangat longgar, hanya sanity check ---
    assert put_qps > 5, f"PUT throughput terlalu rendah: {put_qps:.1f} qps"
    assert get_qps > 20, f"GET throughput terlalu rendah: {get_qps:.1f} qps"

    # Log ringkas agar mudah dibaca di terminal/CI
    print(f"\nPUT QPS≈{put_qps:.1f} | GET QPS≈{get_qps:.1f} | coherence=OK", flush=True)
