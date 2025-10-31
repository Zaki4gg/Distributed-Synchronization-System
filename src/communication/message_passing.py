import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.05, max=1))
async def post_json(url: str, payload: dict, timeout=1.5) -> dict:
    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, json=payload, timeout=timeout) as r:
            r.raise_for_status()
            return await r.json()

async def get_json(url: str, timeout=1.5) -> dict:
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, timeout=timeout) as r:
            r.raise_for_status()
            return await r.json()