# benchmarks/load_test_scenarios.py
# Locust multi-host: Queue + Cache + Lock dalam satu dashboard
# Jalankan UI:
#   $env:QUEUE_HOST="http://localhost:8182"
#   $env:CACHE_HOST="http://localhost:8280"
#   $env:LOCK_HOST="http://localhost:8081"  # arahkan ke leader
#   $env:QUEUE_WEIGHT="3"; $env:CACHE_WEIGHT="2"; $env:LOCK_WEIGHT="1"
#   locust -f benchmarks\load_test_scenarios.py
#
# Headless contoh:
#   locust -f benchmarks\load_test_scenarios.py --headless -u 120 -r 30 -t 2m --csv bench_all
#
# Filter di UI pakai tag: queue / cache / lock

import os, random
from locust import HttpUser, task, between, tag

# --- Konfigurasi umum (ENV override bila perlu) ---
QUEUE_TOPIC     = os.getenv("QUEUE_TOPIC", "bench_alpha")
QUEUE_KEY       = os.getenv("QUEUE_KEY", "user_bench")
QUEUE_VIS_TTL   = int(os.getenv("QUEUE_VIS_TTL_MS", "3000"))

QUEUE_HOST      = os.getenv("QUEUE_HOST", "http://localhost:8182")
CACHE_HOST      = os.getenv("CACHE_HOST", "http://localhost:8280")
LOCK_HOST       = os.getenv("LOCK_HOST",  "http://localhost:8081")  # leader

QUEUE_WEIGHT    = int(os.getenv("QUEUE_WEIGHT", "3"))
CACHE_WEIGHT    = int(os.getenv("CACHE_WEIGHT", "2"))
LOCK_WEIGHT     = int(os.getenv("LOCK_WEIGHT",  "1"))

CACHE_PCT_GET   = int(os.getenv("LOCUST_CACHE_GET",  "80"))
CACHE_MAX_KEYS  = int(os.getenv("LOCUST_CACHE_KEYS", "5000"))
LOCK_RES_COUNT  = int(os.getenv("LOCUST_LOCK_RES",   "12"))

# --- Queue: publish + consume+ack (ack via ack_owner) ---
class QueueUser(HttpUser):
    host = QUEUE_HOST
    weight = max(0, QUEUE_WEIGHT)
    wait_time = between(0.01, 0.05)

    @tag("queue")
    @task(3)
    def publish(self):
        self.client.post(
            "/queue/publish",
            params={"topic": QUEUE_TOPIC, "key": QUEUE_KEY},
            json={"i": random.randint(1, 1_000_000)},
            name="queue:publish"
        )

    @tag("queue")
    @task(1)
    def consume_and_ack(self):
        r = self.client.post(
            "/queue/consume",
            params={"topic": QUEUE_TOPIC, "key": QUEUE_KEY, "visibility_ttl": QUEUE_VIS_TTL, "max": 10},
            name="queue:consume"
        )
        if r.status_code != 200:
            return
        items = r.json() or []
        for it in items:
            self.client.post(
                "/queue/ack_owner",
                params={"topic": QUEUE_TOPIC, "owner": it.get("owner"), "msg_id": it.get("msg_id")},
                name="queue:ack_owner"
            )

# --- Cache: campur GET/PUT sederhana ---
class CacheUser(HttpUser):
    host = CACHE_HOST
    weight = max(0, CACHE_WEIGHT)
    wait_time = between(0.01, 0.05)
    pct_get = CACHE_PCT_GET
    max_keys = CACHE_MAX_KEYS

    @tag("cache")
    @task
    def work(self):
        k = f"k{random.randint(1, self.max_keys)}"
        if random.randint(1, 100) <= self.pct_get:
            self.client.get("/cache/get", params={"key": k}, name="cache:get")
        else:
            self.client.post("/cache/put", json={"key": k, "value": random.randint(1, 1_000_000)}, name="cache:put")

# --- Lock: exclusive acquire + release ke leader ---
class LockUser(HttpUser):
    host = LOCK_HOST
    weight = max(0, LOCK_WEIGHT)
    wait_time = between(0.01, 0.03)
    resources = [f"res_{i}" for i in range(LOCK_RES_COUNT)]

    @tag("lock")
    @task
    def exclusive_lock_cycle(self):
        res = random.choice(self.resources)
        r = self.client.post(
            "/lock/acquire",
            json={"resource": res, "mode": "exclusive", "client_id": "locust", "timeout_ms": 2000},
            name="lock:acquire_exclusive"
        )
        if r.status_code == 200 and (r.json() or {}).get("granted"):
            tok = r.json().get("token")
            self.client.post("/lock/release", json={"resource": res, "token": tok}, name="lock:release")
