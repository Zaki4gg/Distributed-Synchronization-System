# src/nodes/lock_manager.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict, Set, List, Optional

from fastapi import APIRouter, FastAPI, HTTPException, Body
from utils.config import get_settings
from consensus.raft import RaftNode
from communication.message_passing import post_json, get_json


# =========================
# In-memory Lock Table RSM
# =========================

class LockTable:
    """
    Resource lock with shared/exclusive modes + FIFO queue + simple deadlock detection.

    Struktur:
      table: {
        resource: {
          "mode": "shared"|"exclusive"|None,
          "holders": set[(client_id, token)],
          "queue": [{"client": str, "mode": "shared"|"exclusive"} ...]
        }
      }
      waits: wait-for graph; client -> set(client_yang_ditunggu)
    """
    def __init__(self):
        self.table: Dict[str, dict] = {}
        self.waits: Dict[str, Set[str]] = {}
        self._clock = 0  # untuk token sederhana (t1, t2, ...)

    def _new_token(self) -> str:
        self._clock += 1
        return f"t{self._clock}"

    def _holders_of(self, resource: str) -> Set[str]:
        return {c for c, _ in self.table.get(resource, {}).get("holders", set())}

    def apply(self, cmd: dict) -> dict:
        """
        Terapkan perintah ke state machine.
        Return value hanya dikembalikan ke klien jika node leader punya pending future (lihat LockRSM).
        """
        op = cmd.get("op")

        if op == "acquire":
            r = cmd["resource"]; mode = cmd["mode"]; client = cmd["client_id"]
            ent = self.table.setdefault(r, {"mode": None, "holders": set(), "queue": []})

            # Free: langsung grant
            if not ent["holders"]:
                ent["mode"] = mode
                tok = self._new_token()
                ent["holders"].add((client, tok))
                return {"granted": True, "token": tok, "mode": mode, "resource": r}

            # Shared compatible
            if ent["mode"] == "shared" and mode == "shared":
                tok = self._new_token()
                ent["holders"].add((client, tok))
                return {"granted": True, "token": tok, "mode": "shared", "resource": r}

            # Otherwise: queue
            ent["queue"].append({"client": client, "mode": mode})
            # Update wait-for graph
            self.waits.setdefault(client, set()).update(self._holders_of(r))
            return {"granted": False, "queued": True, "resource": r, "mode": mode}

        elif op == "release":
            r = cmd["resource"]; token = cmd["token"]
            ent = self.table.get(r)
            if not ent:
                return {"released": False, "reason": "no_resource"}

            before = set(ent["holders"])
            ent["holders"] = {(c, t) for (c, t) in ent["holders"] if t != token}
            changed = before != ent["holders"]

            if not ent["holders"]:
                ent["mode"] = None
                # Grant queued requests: batch for shared-at-head; else single exclusive
                if ent["queue"]:
                    head = ent["queue"][0]
                    if head["mode"] == "shared":
                        ent["mode"] = "shared"
                        granted_batch = []
                        while ent["queue"] and ent["queue"][0]["mode"] == "shared":
                            req = ent["queue"].pop(0)
                            tok = self._new_token()
                            ent["holders"].add((req["client"], tok))
                            granted_batch.append({"client": req["client"], "token": tok})
                            # Bersihkan wait-for edges untuk req ini
                            self.waits.pop(req["client"], None)
                        return {"released": bool(changed), "granted_batch": granted_batch, "resource": r, "mode": "shared"}

                    else:
                        req = ent["queue"].pop(0)
                        ent["mode"] = "exclusive"
                        tok = self._new_token()
                        ent["holders"].add((req["client"], tok))
                        self.waits.pop(req["client"], None)
                        return {"released": bool(changed), "granted": {"client": req["client"], "token": tok}, "resource": r, "mode": "exclusive"}

            return {"released": bool(changed), "resource": r}

        elif op == "deadlock_probe":
            # Cycle detection (DFS)
            visited, stack = set(), set()

            def dfs(u: str) -> bool:
                visited.add(u); stack.add(u)
                for v in self.waits.get(u, set()):
                    if v not in visited and dfs(v):
                        return True
                    if v in stack:
                        return True
                stack.remove(u)
                return False

            dead = any(dfs(u) for u in list(self.waits.keys()) if u not in visited)
            return {"deadlock": bool(dead)}

        # Unknown op -> no-op
        return {"ok": True}


# =========================
# Lock RSM wrapper
# =========================

@dataclass
class _Pending:
    fut: asyncio.Future


class LockRSM:
    def __init__(self):
        self.table = LockTable()
        self.raft: Optional[RaftNode] = None
        # req_id -> future (leader only)
        self._pending: Dict[str, _Pending] = {}

    async def apply_cmd(self, cmd: dict) -> dict:
        """
        Dipanggil oleh Raft saat commit entry. Kembalikan hasil apply().
        Jika ada req_id yang sedang menunggu (di leader), resolve future-nya.
        """
        res = self.table.apply(cmd)
        req_id = cmd.get("req_id")
        if req_id and req_id in self._pending:
            p = self._pending.pop(req_id, None)
            if p and not p.fut.done():
                p.fut.set_result(res)
        return res

    def _register_wait(self, req_id: str) -> asyncio.Future:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[req_id] = _Pending(fut=fut)
        return fut

    def _resolve_if_waiting(self, req_id: str, result: dict):
        p = self._pending.pop(req_id, None)
        if p and not p.fut.done():
            p.fut.set_result(result)


# =========================
# Helpers
# =========================

async def _find_leader_url(cluster_nodes: List[str]) -> Optional[str]:
    """
    Tanyakan tiap URL /raft/leader; yang balas role=leader adalah leader URL.
    """
    for url in cluster_nodes:
        try:
            j = await get_json(f"{url}/raft/leader", timeout=1.0)
            if j.get("role") == "leader":
                return url
        except Exception:
            pass
    return None


# =========================
# Routes mount
# =========================

async def mount_lock_routes(app: FastAPI) -> LockRSM:
    r = APIRouter()
    settings = get_settings()
    rsm = LockRSM()

    # Build peers list = semua cluster nodes kecuali diri sendiri (by port)
    peers = [p for p in settings.cluster_nodes if f":{settings.http_port}" not in p]
    rsm.raft = RaftNode(settings.node_id, peers, rsm.apply_cmd)
    await rsm.raft.start()

    # ----- Client API -----

    @r.post("/lock/acquire")
    async def acquire(body: dict = Body(...)):
        """
        Request:
          {resource, mode: "shared"|"exclusive", client_id, timeout_ms?}
        Response (leader, setelah commit):
          - Granted segera: {"granted": true, "token": "...", ...}
          - Di-queue : {"granted": false, "queued": true, ...}
        Follower akan auto-proxy ke leader.
        """
        # Follower -> proxy
        if rsm.raft.role != "leader":
            leader = await _find_leader_url(settings.cluster_nodes)
            if not leader:
                raise HTTPException(503, "No leader available")
            return await post_json(f"{leader}/lock/acquire", body, timeout=2.5)

        # Leader path: submit + tunggu result commit
        import uuid
        req_id = body.get("req_id") or str(uuid.uuid4())
        body = dict(body, op="acquire", req_id=req_id)
        fut = rsm._register_wait(req_id)

        ok = await rsm.raft.submit(body)
        if not ok:
            rsm._resolve_if_waiting(req_id, {"error": "replication_failed"})
            raise HTTPException(503, "Replication failed")

        try:
            # tunggu hasil apply (commit) max 2s
            res = await asyncio.wait_for(fut, timeout=2.0)
            return res
        except asyncio.TimeoutError:
            # fallback: accepted tapi belum dapat hasil (client bisa polling /lock/state)
            return {"accepted": True, "pending": True}

    @r.post("/lock/release")
    async def release(body: dict = Body(...)):
        """
        Request: {resource, token}
        Response (leader, setelah commit):
          {"released": true/false, ...}
        Follower auto-proxy.
        """
        if rsm.raft.role != "leader":
            leader = await _find_leader_url(settings.cluster_nodes)
            if not leader:
                raise HTTPException(503, "No leader available")
            return await post_json(f"{leader}/lock/release", body, timeout=2.5)

        import uuid
        req_id = str(uuid.uuid4())
        cmd = dict(body, op="release", req_id=req_id)
        fut = rsm._register_wait(req_id)

        ok = await rsm.raft.submit(cmd)
        if not ok:
            rsm._resolve_if_waiting(req_id, {"error": "replication_failed"})
            raise HTTPException(503, "Replication failed")

        try:
            res = await asyncio.wait_for(fut, timeout=2.0)
            return res
        except asyncio.TimeoutError:
            return {"accepted": True, "pending": True}

    @r.post("/lock/deadlock_probe")
    async def deadlock_probe():
        """
        Jalankan deteksi deadlock terdistribusi (di RSM) dan kembalikan flag.
        """
        if rsm.raft.role != "leader":
            leader = await _find_leader_url(settings.cluster_nodes)
            if not leader:
                raise HTTPException(503, "No leader available")
            return await post_json(f"{leader}/lock/deadlock_probe", {}, timeout=2.0)

        import uuid
        req_id = str(uuid.uuid4())
        cmd = {"op": "deadlock_probe", "req_id": req_id}
        fut = rsm._register_wait(req_id)

        ok = await rsm.raft.submit(cmd)
        if not ok:
            rsm._resolve_if_waiting(req_id, {"error": "replication_failed"})
            raise HTTPException(503, "Replication failed")

        try:
            res = await asyncio.wait_for(fut, timeout=2.0)
            return res
        except asyncio.TimeoutError:
            return {"accepted": True, "pending": True}

    @r.get("/lock/state")
    async def lock_state(resource: Optional[str] = None):
        """
        Debug state: lihat holders/tokens & queue.
        """
        tab = rsm.table.table

        def fmt(ent: dict) -> dict:
            holders = [{"client": c, "token": t} for (c, t) in sorted(list(ent.get("holders", set())))]
            queue = [{"client": q.get("client"), "mode": q.get("mode")} for q in ent.get("queue", [])]
            return {"mode": ent.get("mode"), "holders": holders, "queue": queue}

        if resource:
            ent = tab.get(resource)
            return {resource: fmt(ent)} if ent else {}
        return {r: fmt(ent) for r, ent in tab.items()}

    # ----- Raft RPCs -----

    @r.post("/raft/request_vote")
    async def request_vote(body: dict = Body(...)):
        return await rsm.raft.handle_request_vote(**body)

    @r.post("/raft/append_entries")
    async def append_entries(body: dict = Body(...)):
        return await rsm.raft.handle_append_entries(**body)

    @r.get("/raft/leader")
    async def leader():
        # catatan: leader di sini adalah node_id (bukan URL)
        return {"role": rsm.raft.role, "leader": rsm.raft.leader_url}

    app.include_router(r)
    return rsm
