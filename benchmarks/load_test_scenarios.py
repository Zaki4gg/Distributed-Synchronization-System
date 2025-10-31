import asyncio, json, time, random, httpx, click

async def discover_leader(nodes: list[str]) -> str:
    for _ in range(10):
        for n in nodes:
            try:
                async with httpx.AsyncClient() as c:
                    r = await c.get(f"{n}/raft/leader", timeout=1.0)
                    if r.status_code == 200:
                        j = r.json()
                        if j.get("role") == "leader":
                            return n
            except Exception:
                pass
        await asyncio.sleep(0.2)
    return nodes[0]

async def spam_locks(leader: str, duration: int, rate: int):
    st = time.time(); sent = 0; ok = 0; err = 0
    async with httpx.AsyncClient() as c:
        while time.time() - st < duration:
            t0 = time.time()
            # send `rate` reqs per second
            for _ in range(rate):
                res = random.randint(1, 100)
                try:
                    r = await c.post(f"{leader}/lock/acquire", json={
                        "resource": f"res-{res}",
                        "mode": "shared" if res % 2 else "exclusive",
                        "client_id": f"cli-{random.randint(1,50)}",
                        "timeout_ms": 100
                    }, timeout=1.0)
                    ok += 1 if r.status_code < 400 else 0
                except Exception:
                    err += 1
                sent += 1
            await asyncio.sleep(max(0, 1 - (time.time()-t0)))
    return {"sent": sent, "ok": ok, "err": err}

@click.command()
@click.option("--nodes", type=str, required=True, help="Comma separated lock node URLs")
@click.option("--duration", type=int, default=10)
@click.option("--rate", type=int, default=100)
async def main(nodes, duration, rate):
    ns = nodes.split(",")
    leader = await discover_leader(ns)
    res = await spam_locks(leader, duration, rate)
    print(json.dumps(res))

if __name__ == "__main__":
    asyncio.run(main())