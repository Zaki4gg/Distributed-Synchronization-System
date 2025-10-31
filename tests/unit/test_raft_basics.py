import pytest
from consensus.raft import RaftNode

@pytest.mark.asyncio
async def test_election_single_node():
    applied = []
    async def apply(cmd): applied.append(cmd)
    n = RaftNode("n1", [], apply)
    await n.start()
    await n.submit({"op": "noop"})  # single node acts as leader
    assert n.role == "leader"