# src/nodes/cache_node.py
from __future__ import annotations
import os, json, time, asyncio
from dataclasses import dataclass
from typing import Optional, Any, Dict, Tuple, List
from collections import OrderedDict

from fastapi import FastAPI, APIRouter, Body, Query
import redis.asyncio as redis
from utils.config import get_settings

# =========================
# Settings
# =========================
def _get_env_list(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]

class _Settings:
    def __init__(self):
        s = get_settings()
        self.node_id = getattr(s, "node_id", os.getenv("NODE_ID", "cache-unk"))
        self.http_port = int(getattr(s, "http_port", int(os.getenv("HTTP_PORT", "8280"))))
        self.redis_url = getattr(s, "redis_url", os.getenv("REDIS_URL", "redis://redis:6379/0"))
        self.cache_capacity = int(getattr(s, "cache_capacity", int(os.getenv("CACHE_CAPACITY", "512"))))
        self.cluster_peers = list(getattr(s, "cluster_peers", _get_env_list("CLUSTER_PEERS")))
        self.self_url = f"http://{self.node_id}:{self.http_port}"

SET = _Settings()

# =========================
# Redis keys & helpers
# =========================
def k_data(key: str) -> str: return f"cache:data:{key}"
INV_CHANNEL = "cache:inv"

# =========================
# MESI States (simplified)
# =========================
MESI_I = "I"  # Invalid
MESI_S = "S"  # Shared
MESI_E = "E"  # Exclusive (unused in this simplified write-through)
MESI_M = "M"  # Modified (owner/writer, but we still write-through)

@dataclass
class Entry:
    value: Any
    state: str
    expire_at_ms: Optional[int] = None  # local TTL deadline

# =========================
# LRU Cache (in-memory)
# =========================
class LRU:
    def __init__(self, capacity: int):
        self.capacity = max(1, capacity)
        self._store: "OrderedDict[str, Entry]" = OrderedDict()
        self.evictions = 0

    def _is_expired(self, e: Entry) -> bool:
        return e.expire_at_ms is not None and e.expire_at_ms <= int(time.time() * 1000)

    def get(self, key: str) -> Optional[Entry]:
        e = self._store.get(key)
        if e is None:
            return None
        if self._is_expired(e) or e.state == MESI_I:
            self._store.pop(key, None)
            return None
        self._store.move_to_end(key, last=True)  # mark as MRU
        return e

    def put(self, key: str, entry: Entry):
        if key in self._store:
            self._store.pop(key, None)
        elif len(self._store) >= self.capacity:
            self._store.popitem(last=False)  # evict LRU
            self.evictions += 1
        self._store[key] = entry

    def invalidate(self, key: str):
        e = self._store.get(key)
        if e:
            e.state = MESI_I
            self._store.pop(key, None)

    def state_of(self, key: str) -> Optional[str]:
        e = self._store.get(key)
        return e.state if e else None

# =========================
# Cache Node
# =========================
class CacheNode:
    def __init__(self, r: redis.Redis, capacity: int):
        self.r = r
        self.cache = LRU(capacity)
        self.metrics: Dict[str, int] = dict(
            hits=0, misses=0, puts=0, evictions=0, inv_sent=0, inv_recv=0
        )

    async def write_through(self, key: str, value: Any, ttl_ms: Optional[int]):
        encoded = json.dumps(value, separators=(",", ":"))
        if ttl_ms and ttl_ms > 0:
            await self.r.psetex(k_data(key), ttl_ms, encoded)
        else:
            await self.r.set(k_data(key), encoded)
        expire_at = int(time.time() * 1000) + ttl_ms if ttl_ms and ttl_ms > 0 else None
        self.cache.put(key, Entry(value=value, state=MESI_M, expire_at_ms=expire_at))
        self.metrics["puts"] += 1
        self.metrics["evictions"] = self.cache.evictions

    async def read_through(self, key: str) -> Tuple[bool, Optional[Any]]:
        # 1) coba dari cache lokal
        e = self.cache.get(key)
        if e:
            self.metrics["hits"] += 1
            return True, e.value

        # 2) fetch dari Redis (backing store)
        rkey = k_data(key)
        raw = await self.r.get(rkey)
        if raw is None:
            self.metrics["misses"] += 1
            return False, None

        try:
            val = json.loads(raw)
        except Exception:
            val = raw  # jika bukan JSON

        # Ambil sisa TTL dari Redis (ms) supaya entry lokal juga ikut expire
        ttl_ms = await self.r.pttl(rkey)  # -2: no key, -1: no TTL, >=0: remaining ms
        if ttl_ms is not None and ttl_ms >= 0:
            expire_at = int(time.time() * 1000) + ttl_ms
        else:
            expire_at = None

        self.cache.put(key, Entry(value=val, state=MESI_S, expire_at_ms=expire_at))
        self.metrics["misses"] += 1
        return True, val

    async def broadcast_invalidate(self, key: str):
        msg = {"op": "inv", "key": key, "from": SET.self_url, "ts": int(time.time() * 1000)}
        await self.r.publish(INV_CHANNEL, json.dumps(msg))
        self.metrics["inv_sent"] += 1

    def apply_invalidate_local(self, key: str):
        self.cache.invalidate(key)
        self.metrics["inv_recv"] += 1

# =========================
# Mount routes
# =========================
async def mount_cache_routes(app: FastAPI):
    r = APIRouter()
    rds = redis.from_url(SET.redis_url, decode_responses=True)
    node = CacheNode(rds, capacity=SET.cache_capacity)

    # Background: listen invalidations
    async def inv_listener():
        await asyncio.sleep(0.2)
        pub = rds.pubsub()
        await pub.subscribe(INV_CHANNEL)
        try:
            while True:
                msg = await pub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and isinstance(msg.get("data"), str):
                    try:
                        data = json.loads(msg["data"])
                        if data.get("op") == "inv" and data.get("from") != SET.self_url:
                            key = data.get("key")
                            if key:
                                node.apply_invalidate_local(key)
                    except Exception:
                        pass
                await asyncio.sleep(0.05)
        finally:
            try:
                await pub.unsubscribe(INV_CHANNEL)
                await pub.close()
            except Exception:
                pass

    app.add_event_handler("startup", lambda: asyncio.create_task(inv_listener()))

    @r.get("/readyz")
    async def readyz():
        try:
            await rds.ping()
            return {"ready": True, "self": SET.self_url}
        except Exception:
            return {"ready": False, "self": SET.self_url}

    # PUT: write-through + broadcast invalidation (MESI write-invalidate)
    @r.post("/cache/put")
    async def cache_put(
        body: Dict[str, Any] = Body(...),
        ttl_ms: Optional[int] = Query(None)
    ):
        key = body.get("key")
        value = body.get("value")
        if not isinstance(key, str):
            return {"stored": False, "error": "key required"}
        await node.write_through(key, value, ttl_ms)
        await node.broadcast_invalidate(key)
        return {"stored": True}

    # GET: read-through (cache S), TTL lokal mengikuti TTL Redis
    @r.get("/cache/get")
    async def cache_get(key: str):
        hit, val = await node.read_through(key)
        if not hit:
            return {"hit": False}
        return {"hit": True, "value": val}

    # Manual invalidation (opsional)
    @r.post("/cache/invalidate")
    async def cache_invalidate(key: str):
        node.apply_invalidate_local(key)
        await node.broadcast_invalidate(key)
        return {"invalidated": True}

    # Debug state
    @r.get("/cache/state")
    async def cache_state(key: str):
        st = node.cache.state_of(key)
        return {"key": key, "state": st}

    # Metrics
    @r.get("/cache/metrics")
    async def cache_metrics():
        return {
            "node": SET.self_url,
            "metrics": node.metrics,
            "capacity": SET.cache_capacity,
            "evictions": node.cache.evictions
        }

    app.include_router(r)
