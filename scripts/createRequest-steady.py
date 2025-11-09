import aiohttp
import asyncio
import time
from datetime import datetime

URL = "http://localhost:9111/api/v1/monitor/45"   # Smart autoscaler endpoint
CONCURRENT = 3            # only 3 requests at once → safe for cluster
RUN_TIME = 600            # seconds (10 min)
DELAY_BETWEEN = 0.2       # seconds between new requests
LOG_INTERVAL = 10         # summary interval

async def fetch(session, sem, i, results):
    async with sem:
        start = time.perf_counter()
        try:
            async with session.get(URL, timeout=60) as resp:
                await resp.text()
                latency = (time.perf_counter() - start) * 1000
                results.append(latency)
        except Exception:
            results.append(None)

async def main():
    sem = asyncio.Semaphore(CONCURRENT)
    results = []
    start_time = time.perf_counter()
    last_log = start_time
    i = 0

    print(f"🚀 Running safe steady load on {URL}")
    print(f"   concurrency={CONCURRENT}, duration={RUN_TIME}s\n")

    async with aiohttp.ClientSession() as session:
        while True:
            now = time.perf_counter()
            if now - start_time > RUN_TIME:
                break

            i += 1
            asyncio.create_task(fetch(session, sem, i, results))
            await asyncio.sleep(DELAY_BETWEEN)

            # summary every LOG_INTERVAL seconds
            if now - last_log >= LOG_INTERVAL:
                done = [r for r in results if r is not None]
                ok = len(done)
                avg = sum(done) / ok if ok else 0
                rps = ok / LOG_INTERVAL
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"📊 {ts} | RPS={rps:.2f} | avg={avg:.1f} ms | ok={ok}")
                results.clear()
                last_log = now

    print("\n✅ Safe load complete – about 10 min run finished.")

if __name__ == "__main__":
    asyncio.run(main())