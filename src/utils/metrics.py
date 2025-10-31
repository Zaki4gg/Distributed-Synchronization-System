from prometheus_client import Counter, Histogram

REQS = Counter("http_requests_total", "Total HTTP requests", ["service", "endpoint", "status"])
LAT = Histogram("http_request_latency_seconds", "Latency", ["service", "endpoint"])
RAFT_TERM = Counter("raft_term_changes_total", "Term changes")
RAFT_APPEND = Counter("raft_append_entries_total", "AppendEntries calls", ["result"])  # ok|fail
QUEUE_PUB = Counter("queue_publish_total", "Messages published", ["topic"])
QUEUE_ACK = Counter("queue_ack_total", "Acks", ["topic"])
CACHE_HIT = Counter("cache_hits_total", "Cache hits", ["node"])
CACHE_MISS = Counter("cache_miss_total", "Cache misses", ["node"])