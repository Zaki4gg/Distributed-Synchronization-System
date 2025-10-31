# src/nodes/base_node.py
from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
from typing import Dict, Optional

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

from utils.config import get_settings
from utils.metrics import REQS, LAT
from communication.message_passing import get_json
from communication.failure_detector import (
    PhiAccrualFailureDetector,
    periodic_probe,
)

# Komponen per-role
from nodes.lock_manager import mount_lock_routes, LockRSM
from nodes.queue_node import mount_queue_routes
from nodes.cache_node import mount_cache_routes

# Prometheus /metrics
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

# ------------------------------------------------------------------------------
# Logging & App
# ------------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("distributed-sync.base")

app = FastAPI(title="Distributed Sync System", version="0.1.0")

# CORS longgar untuk demo
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

settings = get_settings()

# State global
lock_rsm: Optional[LockRSM] = None
_fd: Optional[PhiAccrualFailureDetector] = None
_fd_task: Optional[asyncio.Task] = None
_ready: bool = False  # readiness flag


# ------------------------------------------------------------------------------
# Helper: Build daftar peers untuk Failure Detector
# ------------------------------------------------------------------------------
def _build_peers_for_role() -> Dict[str, str]:
    """
    Kembalikan mapping {peer_id: base_url} untuk diprobe oleh failure detector.
    - LOCK: gunakan CLUSTER_NODES (http://lock1:8080, http://lock2:8081, ...)
    - QUEUE/CACHE: opsional gunakan CLUSTER_PEERS (http URL, dipisah koma)
    Peer self akan di-skip (berdasar HTTP_PORT) bila memungkinkan.
    """
    role = settings.role
    peers: Dict[str, str] = {}

    urls: list[str] = []
    if role == "lock":
        urls = settings.cluster_nodes or []
    else:
        env_peers = os.getenv("CLUSTER_PEERS", "")
        if env_peers.strip():
            urls = [u.strip() for u in env_peers.split(",") if u.strip()]

    if not urls:
        return peers

    for idx, url in enumerate(urls, start=1):
        # Skip diri sendiri jika terdeteksi lewat port
        try:
            port_str = f":{settings.http_port}"
            if port_str in url:
                continue
        except Exception:
            pass

        # Peer id: gunakan host:port bila bisa di-parse, kalau tidak pakai alias peer{idx}
        pid = _hostport_from_url(url) or f"peer{idx}"
        # Pastikan unik
        base_pid = pid
        dup = 1
        while pid in peers:
            pid = f"{base_pid}-{dup}"
            dup += 1
        peers[pid] = url

    return peers


def _hostport_from_url(url: str) -> Optional[str]:
    """
    Ambil 'host:port' dari URL sederhana tanpa pakai urllib (menghindari dep).
    Return None jika gagal parse.
    """
    try:
        # Skema optional
        s = url
        if "://" in s:
            s = s.split("://", 1)[1]
        # Hilangkan path
        s = s.split("/", 1)[0]
        # Jika tanpa port, tetap kembalikan host
        return s
    except Exception:
        return None


# ------------------------------------------------------------------------------
# Lifecycle
# ------------------------------------------------------------------------------
@app.on_event("startup")
async def on_startup() -> None:
    global lock_rsm, _fd, _fd_task, _ready

    logger.info(
        "Starting node role=%s id=%s port=%s",
        settings.role,
        settings.node_id,
        settings.http_port,
    )

    # Mount role-specific routes
    if settings.role == "lock":
        lock_rsm = await mount_lock_routes(app)
        logger.info("Lock routes mounted (Raft RSM).")
    elif settings.role == "queue":
        await mount_queue_routes(app)
        logger.info("Queue routes mounted.")
    elif settings.role == "cache":
        await mount_cache_routes(app)
        logger.info("Cache routes mounted.")
    else:
        logger.warning("Unknown ROLE=%s; no routes mounted.", settings.role)

    # Failure detector
    peers = _build_peers_for_role()
    if peers:
        _fd = PhiAccrualFailureDetector(phi_threshold_default=8.0)
        async def _probe(url: str):
            # sukses bila /health 200
            return await get_json(f"{url}/health", timeout=1.0)

        def _on_change(pid: str, avail: bool, phi: float) -> None:
            logger.info("[FD] %s -> %s (phi=%.2f)", pid, "UP" if avail else "DOWN", phi)

        _fd_task = asyncio.create_task(
            periodic_probe(
                peers=peers,
                fd=_fd,
                probe_func=_probe,
                interval=0.5,
                timeout=1.0,
                threshold=8.0,
                on_change=_on_change,
            )
        )
        logger.info("Failure detector started; peers=%s", list(peers.keys()))
    else:
        logger.info("No peers configured for failure detector.")

    _ready = True
    logger.info("Startup complete. Ready.")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global _fd_task, lock_rsm
    logger.info("Shutting down...")

    # Stop FD task
    if _fd_task:
        _fd_task.cancel()
        try:
            await _fd_task
        except asyncio.CancelledError:
            pass
        _fd_task = None

    # Gracefully stop Raft (lock role)
    try:
        if settings.role == "lock" and lock_rsm and lock_rsm.raft:
            await lock_rsm.raft.stop()
    except Exception as e:
        logger.exception("Error while stopping Raft: %s", e)

    logger.info("Shutdown complete.")


# ------------------------------------------------------------------------------
# Routes: health / ready / metrics / whoami / fd
# ------------------------------------------------------------------------------
@app.get("/health")
async def health() -> dict:
    REQS.labels(service=settings.role, endpoint="/health", status="200").inc()
    return {
        "status": "ok",
        "role": settings.role,
        "node_id": settings.node_id,
        "hostname": socket.gethostname(),
        "time": time.time(),
    }


@app.get("/readyz")
async def readyz() -> dict:
    # Readiness sederhana: app fully started, dan (opsional) lock_rsm siap
    ok = _ready
    if settings.role == "lock" and (lock_rsm is None or lock_rsm.raft is None):
        ok = False
    status = "ok" if ok else "not_ready"
    REQS.labels(service=settings.role, endpoint="/readyz", status="200").inc()
    return {"ready": ok, "status": status}


@app.get("/metrics")
async def metrics() -> Response:
    # Tidak pakai REQS/LAT untuk /metrics agar menghindari efek refleksi
    data = generate_latest()  # default registry
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@app.get("/whoami")
async def whoami() -> dict:
    REQS.labels(service=settings.role, endpoint="/whoami", status="200").inc()
    return {
        "role": settings.role,
        "node_id": settings.node_id,
        "http_port": settings.http_port,
        "cluster_nodes": settings.cluster_nodes,
        "redis_url": settings.redis_url,
    }


@app.get("/fd/status")
async def fd_status() -> dict:
    """
    Tampilkan snapshot FD: phi, available, last_heartbeat_age_ms.
    Jika peers kosong atau FD tidak aktif, kembalikan objek kosong.
    """
    REQS.labels(service=settings.role, endpoint="/fd/status", status="200").inc()
    if _fd is None:
        return {"peers": {}, "active": False}

    snap = _fd.snapshot()
    now_ms = time.monotonic() * 1000.0
    peers_out: Dict[str, dict] = {}
    for pid, st in snap.items():
        phi_val = _fd.phi(pid, now_ms=now_ms)
        last_ts = st.get("last_ts_ms")
        age_ms = (now_ms - last_ts) if last_ts is not None else None
        peers_out[pid] = {
            "phi": round(float(phi_val), 3),
            "available": phi_val < _fd.phi_threshold_default,
            "last_heartbeat_ms": last_ts,
            "age_ms": round(age_ms, 3) if age_ms is not None else None,
            "mean_ms": round(st.get("mean_ms", 0.0), 3),
            "std_ms": round(st.get("std_ms", 0.0), 3),
        }
    return {"peers": peers_out, "active": True}


# ------------------------------------------------------------------------------
# Simple latency middleware (opsional, untuk contoh)
# ------------------------------------------------------------------------------
@app.middleware("http")
async def _metrics_mw(request, call_next):
    start = time.perf_counter()
    try:
        resp = await call_next(request)
        status = str(resp.status_code)
    except Exception:
        status = "500"
        raise
    finally:
        try:
            dur = time.perf_counter() - start
            path = request.url.path
            REQS.labels(service=settings.role, endpoint=path, status=status).inc()
            LAT.labels(service=settings.role, endpoint=path).observe(dur)
        except Exception:
            # Jangan biarkan metrics ganggu request
            pass
    return resp
