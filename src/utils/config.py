from pydantic import BaseModel
from functools import lru_cache
import os

class Settings(BaseModel):
    role: str = os.getenv("ROLE", "lock")
    node_id: str = os.getenv("NODE_ID", "node1")
    http_port: int = int(os.getenv("HTTP_PORT", "8080"))
    cluster_nodes: list[str] = os.getenv("CLUSTER_NODES", "").split(",") if os.getenv("CLUSTER_NODES") else []
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Raft
    raft_election_lo: int = int(os.getenv("RAFT_ELECTION_LO", "150"))
    raft_election_hi: int = int(os.getenv("RAFT_ELECTION_HI", "300"))
    raft_heartbeat_ms: int = int(os.getenv("RAFT_HEARTBEAT_MS", "75"))

    # Queue
    queue_replica_factor: int = int(os.getenv("QUEUE_REPLICA_FACTOR", "2"))
    visibility_default_ms: int = int(os.getenv("VISIBILITY_DEFAULT_MS", "30000"))

    # Cache
    cache_dir_leader: str = os.getenv("CACHE_DIR_LEADER", "")
    cache_default_ttl_ms: int = int(os.getenv("CACHE_DEFAULT_TTL_MS", "60000"))

@lru_cache
def get_settings() -> Settings:
    return Settings()