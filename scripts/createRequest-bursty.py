import asyncio, aiohttp, random, time

BURST_DURATION = 15        # seconds of high load
COOLDOWN_DURATION = 25     # seconds of normal load
NORMAL_CONCURRENCY = 10    # steady state
BURST_CONCURRENCY = 60     # burst state
REQUEST_DELAY = 0.05       # gap between new requests

async def fetch(url, session, i):
    async with session.get(url) as resp:
        await resp.text()

async def run_phase(url, session, duration, max_concurrency):
    """Generate traffic for a fixed duration with given concurrency."""
    start = time.time()
    tasks = set()
    while time.time() - start < duration:
        if len(tasks) < max_concurrency:
            task = asyncio.create_task(fetch(url, session, random.randint(1, 1_000_000)))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
        else:
            _done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        await asyncio.sleep(REQUEST_DELAY)
    # Wait for all remaining tasks
    if tasks:
        await asyncio.wait(tasks)

async def main():
    url = "http://localhost:9110/api/v1/monitor/45"
    async with aiohttp.ClientSession() as session:
        while True:
            print("[phase] normal load")
            await run_phase(url, session, COOLDOWN_DURATION, NORMAL_CONCURRENCY)
            print("[phase] burst load")
            await run_phase(url, session, BURST_DURATION, BURST_CONCURRENCY)

if __name__ == "__main__":
    asyncio.run(main())
