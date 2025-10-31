from __future__ import annotations
import os, json, time, uuid, asyncio, hashlib
from typing import List, Optional, Tuple

from fastapi import APIRouter, FastAPI, Body, HTTPException, Query
from utils.config import get_settings
from communication.message_passing import post_json, get_json
import redis.asyncio as redis


# =========================
# Settings
# =========================

def _get_env_list(name: str, default: str = "") -> List[str]:
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]

class _Settings:
    def __init__(self):
        s = get_settings()
        self.node_id = getattr(s, "node_id", os.getenv("NODE_ID", "queue-unk"))
        self.http_port = int(getattr(s, "http_port", int(os.getenv("HTTP_PORT", "8180"))))
        self.redis_url = getattr(s, "redis_url", os.getenv("REDIS_URL", "redis://redis:6379/0"))
        self.cluster_peers = list(getattr(s, "cluster_peers", _get_env_list("CLUSTER_PEERS")))
        # pastikan self url masuk peers
        self.self_url = f"http://{self.node_id}:{self.http_port}"
        if self.self_url not in self.cluster_peers:
            self.cluster_peers.append(self.self_url)
        self.replica_factor = int(getattr(s, "queue_replica_factor", int(os.getenv("QUEUE_REPLICA_FACTOR", "2"))))
        self.visibility_default_ms = int(getattr(s, "visibility_default_ms", int(os.getenv("VISIBILITY_DEFAULT_MS", "30000"))))

SET = _Settings()


# =========================
# Consistent Hash Ring
# =========================

def _sha1_int(x: str) -> int:
    return int(hashlib.sha1(x.encode("utf-8")).hexdigest(), 16)

class Ring:
    def __init__(self, peers: List[str], vnodes: int = 64):
        self.points: List[Tuple[int, str]] = []
        for p in peers:
            for v in range(vnodes):
                self.points.append((_sha1_int(f"{p}#{v}"), p))
        self.points.sort(key=lambda t: t[0])

    def owners(self, topic: str, key: Optional[str], r: int) -> List[str]:
        if r <= 0:
            r = 1
        k = key or topic
        h = _sha1_int(f"{topic}|{k}")
        # binary search untuk titik pertama >= h
        lo, hi = 0, len(self.points) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            if self.points[mid][0] < h:
                lo = mid + 1
            else:
                hi = mid - 1
        idx = lo if lo < len(self.points) else 0
        # kumpulkan r peer unik (skip duplikat karena vnodes)
        res, seen = [], set()
        i = idx
        while len(res) < min(r, len(self.points)):
            _, peer = self.points[i]
            if peer not in seen:
                res.append(peer)
                seen.add(peer)
            i = (i + 1) % len(self.points)
            if i == idx:
                break
        return res

RING = Ring(SET.cluster_peers, vnodes=64)


# =========================
# Redis Keyspace Helpers
# =========================

def k_ready(topic: str, owner: str) -> str:    return f"q:{topic}:{owner}:ready"
def k_inflight(topic: str, owner: str) -> str: return f"q:{topic}:{owner}:inflight"
def k_vis(topic: str, owner: str) -> str:      return f"q:{topic}:{owner}:vis"
def k_msg(msg_id: str) -> str:                 return f"q:msg:{msg_id}"
def k_owners(msg_id: str) -> str:              return f"q:owners:{msg_id}"
def k_topics() -> str:                         return "q:topics"


# =========================
# Shard Store (ops per owner)
# =========================

class ShardStore:
    def __init__(self, r: redis.Redis):
        self.r = r  # decode_responses=True -> strings

    async def publish_to_owner(self, topic: str, owner: str, msg_id: str, payload: dict):
        # Simpan payload & topic (sekali; idempotent antar replica oke)
        await self.r.hset(k_msg(msg_id), mapping={
            "payload": json.dumps(payload, separators=(",",":")),
            "topic": topic
        })
        await self.r.sadd(k_owners(msg_id), owner)
        # Masuk ke READY (LPUSH = newest first)
        await self.r.lpush(k_ready(topic, owner), msg_id)

    async def consume_from_owner(self, topic: str, owner: str, max_n: int, ttl_ms: int) -> List[dict]:
        out: List[dict] = []
        now_ms = int(time.time() * 1000)
        for _ in range(max_n):
            # FIFO: RPOPLPUSH dari READY ke INFLIGHT
            mid = await self.r.rpoplpush(k_ready(topic, owner), k_inflight(topic, owner))
            if not mid:
                break
            # set visibility deadline
            await self.r.zadd(k_vis(topic, owner), {mid: now_ms + ttl_ms})
            raw = await self.r.hget(k_msg(mid), "payload")
            payload = json.loads(raw) if raw else None
            out.append({"msg_id": mid, "payload": payload, "owner": owner})
        return out

    async def ack_any_owner(self, topic: str, msg_id: str) -> bool:
        owners = await self.r.smembers(k_owners(msg_id))
        removed_any = False
        for owner in owners:
            # hapus dari inflight ATAU ready (edge-case)
            rem_inflight = await self.r.lrem(k_inflight(topic, owner), 0, msg_id)
            rem_ready    = await self.r.lrem(k_ready(topic, owner),    0, msg_id)
            # bersihkan vis index
            await self.r.zrem(k_vis(topic, owner), msg_id)
            if rem_inflight or rem_ready:
                removed_any = True
        if removed_any:
            await self.r.delete(k_msg(msg_id))
            await self.r.delete(k_owners(msg_id))
        return bool(removed_any)

    async def ack_owner(self, topic: str, owner: str, msg_id: str) -> bool:
        rem_inflight = await self.r.lrem(k_inflight(topic, owner), 0, msg_id)
        rem_ready    = await self.r.lrem(k_ready(topic, owner),    0, msg_id)
        await self.r.zrem(k_vis(topic, owner), msg_id)
        if rem_inflight or rem_ready:
            await self.r.srem(k_owners(msg_id), owner)
            if await self.r.scard(k_owners(msg_id)) == 0:
                await self.r.delete(k_msg(msg_id))
                await self.r.delete(k_owners(msg_id))
            return True
        return False

    async def requeue_expired_for_owner(self, topic: str, owner: str) -> int:
        now_ms = int(time.time() * 1000)
        expired = await self.r.zrangebyscore(k_vis(topic, owner), min="-inf", max=now_ms)
        if not expired:
            return 0
        n = 0
        for mid in expired:
            # pindah dari inflight ke ready lagi
            await self.r.lrem(k_inflight(topic, owner), 0, mid)
            await self.r.lpush(k_ready(topic, owner), mid)
            await self.r.zrem(k_vis(topic, owner), mid)
            n += 1
        return n


# =========================
# Routes
# =========================

async def mount_queue_routes(app: FastAPI):
    r = APIRouter()
    rds = redis.from_url(SET.redis_url, decode_responses=True)
    store = ShardStore(rds)

    # Health
    @r.get("/readyz")
    async def readyz():
        try:
            await rds.ping()
            return {"ready": True, "status": "ok"}
        except Exception:
            return {"ready": False, "status": "redis_unreachable"}

    # FD-lite
    @r.get("/fd/status")
    async def fd_status():
        return {"peers": SET.cluster_peers, "self": SET.self_url}

    # Background requeue worker (hanya kelola shard untuk owner=self)
    async def requeue_worker():
        await asyncio.sleep(1.0)
        while True:
            try:
                topics = await rds.smembers(k_topics())
                for t in topics:
                    try:
                        await store.requeue_expired_for_owner(t, SET.self_url)
                    except Exception:
                        pass
            except Exception:
                pass
            await asyncio.sleep(0.5)

    app.add_event_handler("startup", lambda: asyncio.create_task(requeue_worker()))

    # Helpers
    async def _proxy(url: str, path: str, params: dict | None, body: dict | None, timeout=2.5):
        base = url.rstrip("/")
        q = ""
        if params:
            from urllib.parse import urlencode
            q = "?" + urlencode(params)
        full = f"{base}{path}{q}"
        if body is None:
            return await get_json(full, timeout=timeout)
        return await post_json(full, body, timeout=timeout)

    # Debug: lihat owners untuk (topic, key)
    @r.get("/queue/owners")
    async def owners(topic: str, key: Optional[str] = None):
        return {"owners": RING.owners(topic, key, SET.replica_factor), "self": SET.self_url}

    # ---------- Publish ----------
    @r.post("/queue/publish")
    async def publish(topic: str, key: Optional[str] = None, payload: dict = Body(...)):
        await rds.sadd(k_topics(), topic)
        msg_id = uuid.uuid4().hex
        owners = RING.owners(topic, key, SET.replica_factor)

        for ow in owners:
            if ow == SET.self_url:
                await store.publish_to_owner(topic, ow, msg_id, payload)
            else:
                await _proxy(
                    ow, "/queue/publish_internal",
                    {"topic": topic, "owner": ow},
                    {"msg_id": msg_id, "payload": payload}
                )
        return {"msg_id": msg_id, "owners": owners}

    @r.post("/queue/publish_internal")
    async def publish_internal(topic: str, owner: str, body: dict = Body(...)):
        if owner != SET.self_url:
            raise HTTPException(403, "wrong owner")
        await rds.sadd(k_topics(), topic)
        await store.publish_to_owner(topic, owner, body["msg_id"], body["payload"])
        return {"ok": True}

    # ---------- Consume ----------
    @r.post("/queue/consume")
    async def consume(
        topic: str,
        key: Optional[str] = None,
        visibility_ttl: Optional[int] = None,
        max: int = 1
    ):
        ttl_ms = int(visibility_ttl or SET.visibility_default_ms)
        owners = RING.owners(topic, key, SET.replica_factor)

        # coba primary lalu replicas
        for ow in owners:
            try:
                if ow == SET.self_url:
                    items = await store.consume_from_owner(topic, ow, max, ttl_ms)
                else:
                    items = await _proxy(
                        ow, "/queue/consume_internal",
                        {"topic": topic, "owner": ow, "visibility_ttl": ttl_ms, "max": max},
                        body=None
                    )
                if items:
                    # pastikan list
                    if isinstance(items, dict):
                        items = [items]
                    return items
            except Exception:
                # fallback owner berikutnya
                continue
        return []

    @r.get("/queue/consume_internal")
    async def consume_internal(topic: str, owner: str, visibility_ttl: int, max: int = 1):
        if owner != SET.self_url:
            raise HTTPException(403, "wrong owner")
        items = await store.consume_from_owner(topic, owner, max, visibility_ttl)
        return items

    # ---------- ACK (query compat) ----------
    @r.post("/queue/ack")
    async def ack(topic: str = Query(...), msg_id: str = Query(...)):
        ok = await store.ack_any_owner(topic, msg_id)
        if not ok:
            return {"acked": False, "reason": "not_in_inflight"}
        return {"acked": True}

    # ---------- ACK spesifik owner ----------
    @r.post("/queue/ack_owner")
    async def ack_owner(topic: str = Query(...), owner: str = Query(...), msg_id: str = Query(...)):
        ok = await store.ack_owner(topic, owner, msg_id)
        if not ok:
            return {"acked": False, "reason": "not_in_inflight"}
        return {"acked": True}

    # ---------- ACK tanpa tahu topic (auto) ----------
    @r.post("/queue/ack_any")
    async def ack_any(msg_id: str = Query(...)):
        topic = await rds.hget(k_msg(msg_id), "topic")
        if not topic:
            return {"acked": False, "reason": "unknown_topic"}
        ok = await store.ack_any_owner(topic, msg_id)
        return {"acked": bool(ok)}

    app.include_router(r)
