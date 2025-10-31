import asyncio, random, time
from dataclasses import dataclass, field
from typing import List, Optional, Literal
from utils.config import get_settings
from utils.metrics import RAFT_TERM, RAFT_APPEND
from communication.message_passing import post_json

Role = Literal["follower", "candidate", "leader"]

@dataclass
class LogEntry:
    term: int
    cmd: dict  # {"op": "lock_acquire"|"lock_release", ...}

@dataclass
class RaftNode:
    node_id: str
    peers: List[str]
    apply_fn: callable  # apply(log_entry.cmd) -> None
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)
    commit_index: int = 0
    last_applied: int = 0
    next_index: dict = field(default_factory=dict)
    match_index: dict = field(default_factory=dict)
    role: Role = "follower"
    leader_url: Optional[str] = None

    def __post_init__(self):
        self._election_task = None
        self._hb_task = None
        self._stopping = False
        self._reset_election_deadline()

    def _reset_election_deadline(self):
        s = get_settings()
        self._election_deadline = time.monotonic() + random.uniform(s.raft_election_lo/1000, s.raft_election_hi/1000)

    async def start(self):
        self._election_task = asyncio.create_task(self._election_loop())

    async def stop(self):
        self._stopping = True
        for t in [self._election_task, self._hb_task]:
            if t: t.cancel()

    async def _election_loop(self):
        s = get_settings()
        while not self._stopping:
            await asyncio.sleep(0.01)
            if time.monotonic() >= self._election_deadline and self.role != "leader":
                # start election
                self.role = "candidate"
                self.current_term += 1
                RAFT_TERM.inc()
                self.voted_for = self.node_id
                votes = 1
                self._reset_election_deadline()
                # request votes
                for p in self.peers:
                    try:
                        resp = await post_json(f"{p}/raft/request_vote", {
                            "term": self.current_term,
                            "candidate_id": self.node_id,
                            "last_log_index": len(self.log),
                            "last_log_term": self.log[-1].term if self.log else 0,
                        }, timeout=1.0)
                        if resp.get("vote_granted"):
                            votes += 1
                    except Exception:
                        pass
                if votes > (len(self.peers)+1)//2:
                    # be leader
                    self.role = "leader"
                    self.leader_url = None
                    # init next/match index
                    next_idx = len(self.log) + 1
                    self.next_index = {p: next_idx for p in self.peers}
                    self.match_index = {p: 0 for p in self.peers}
                    # start heartbeats
                    if self._hb_task: self._hb_task.cancel()
                    self._hb_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self):
        s = get_settings()
        while not self._stopping and self.role == "leader":
            await self._broadcast_append_entries()
            await asyncio.sleep(s.raft_heartbeat_ms/1000)

    async def _broadcast_append_entries(self):
        # send empty AppendEntries as heartbeat
        for p in self.peers:
            prev_idx = self.next_index[p] - 1
            prev_term = self.log[prev_idx-1].term if prev_idx-1 >= 0 and self.log else 0
            try:
                resp = await post_json(f"{p}/raft/append_entries", {
                    "term": self.current_term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_idx,
                    "prev_log_term": prev_term,
                    "entries": [],
                    "leader_commit": self.commit_index,
                }, timeout=1.0)
                RAFT_APPEND.labels(result="ok").inc()
            except Exception:
                RAFT_APPEND.labels(result="fail").inc()

    # === public API ===
    async def submit(self, cmd: dict) -> bool:
        if self.role != "leader":
            return False
        self.log.append(LogEntry(self.current_term, cmd))
        await self._replicate()
        return True

    async def _replicate(self):
        # naive: push last entry to peers and wait majority
        index = len(self.log)
        acks = 1
        for p in self.peers:
            prev_idx = index - 1
            prev_term = self.log[prev_idx-1].term if prev_idx-1 >= 0 else 0
            try:
                resp = await post_json(f"{p}/raft/append_entries", {
                    "term": self.current_term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_idx,
                    "prev_log_term": prev_term,
                    "entries": [{"term": self.current_term, "cmd": self.log[-1].cmd}],
                    "leader_commit": self.commit_index,
                }, timeout=1.5)
                if resp.get("success"):
                    acks += 1
            except Exception:
                pass
        if acks > (len(self.peers)+1)//2:
            self.commit_index = index
            await self._apply_commits()

    async def handle_request_vote(self, term:int, candidate_id:str, last_log_index:int, last_log_term:int):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.role = "follower"
        up_to_date = (last_log_term > (self.log[-1].term if self.log else 0)) or \
                     (last_log_term == (self.log[-1].term if self.log else 0) and last_log_index >= len(self.log))
        vote_granted = False
        if (self.voted_for in (None, candidate_id)) and up_to_date and term >= self.current_term:
            self.voted_for = candidate_id
            vote_granted = True
            self._reset_election_deadline()
        return {"term": self.current_term, "vote_granted": vote_granted}

    async def handle_append_entries(self, term:int, leader_id:str, prev_log_index:int, prev_log_term:int, entries:list, leader_commit:int):
        if term < self.current_term:
            return {"term": self.current_term, "success": False}
        self.role = "follower"
        self.leader_url = leader_id
        self._reset_election_deadline()
        # log consistency check
        if prev_log_index > 0:
            if len(self.log) < prev_log_index or (self.log[prev_log_index-1].term != prev_log_term):
                return {"term": self.current_term, "success": False}
        # append any new entries
        for e in entries:
            self.log.append(LogEntry(e["term"], e["cmd"]))
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))
            await self._apply_commits()
        return {"term": self.current_term, "success": True}

    async def _apply_commits(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            cmd = self.log[self.last_applied-1].cmd
            await self.apply_fn(cmd)