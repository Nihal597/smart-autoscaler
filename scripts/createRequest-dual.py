import aiohttp
import asyncio
import time
import csv
import subprocess
import json
from datetime import datetime

# --- CONFIG ---
URLS = {
    "HPA": "http://localhost:9110/api/v1/monitor/45",
    "SMART": "http://localhost:9111/api/v1/monitor/45",
}
CONCURRENT = 3
RUNTIME = 300        # 5 minutes
LOG_INTERVAL = 10    # every 10s
DELAY = 0.2
NAMESPACE = "default"
DEPLOYMENTS = {
    "HPA": "integration-svc-depl-hpa",
    "SMART": "integration-svc-depl-autoscaler",
}
CSV_FILE = "compare_results.csv"
# -------------

async def fetch(session, sem, url, tag, results):
    async with sem:
        start = time.perf_counter()
        try:
            async with session.get(url, timeout=60) as resp:
                await resp.text()
                dur = (time.perf_counter() - start) * 1000
                results.append((tag, True, dur))
        except Exception:
            results.append((tag, False, 0))

def get_replicas(deployment):
    try:
        cmd = [
            "kubectl", "get", "deploy", deployment,
            "-n", NAMESPACE, "-o", "json"
        ]
        out = subprocess.check_output(cmd)
        data = json.loads(out)
        return data["status"].get("replicas", 0)
    except Exception:
        return 0

async def main():
    sems = {tag: asyncio.Semaphore(CONCURRENT) for tag in URLS}
    results = []
    start = time.perf_counter()
    last_log = start

    print("🚀 Starting 5-minute Smart vs HPA comparison...")
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "target", "rps", "avg_latency_ms", "replicas"])

        async with aiohttp.ClientSession() as session:
            while time.perf_counter() - start < RUNTIME:
                for tag, url in URLS.items():
                    asyncio.create_task(fetch(session, sems[tag], url, tag, results))
                await asyncio.sleep(DELAY)

                now = time.perf_counter()
                if now - last_log >= LOG_INTERVAL:
                    ts = datetime.now().strftime("%H:%M:%S")
                    for tag in URLS.keys():
                        subset = [lat for t, ok, lat in results if t == tag and ok]
                        results = [r for r in results if r[0] != tag]
                        if subset:
                            avg_lat = sum(subset) / len(subset)
                            rps = len(subset) / LOG_INTERVAL
                        else:
                            avg_lat, rps = 0, 0
                        replicas = get_replicas(DEPLOYMENTS[tag])
                        print(f"📊 {ts} [{tag}] RPS={rps:.2f}, Lat={avg_lat:.1f}ms, Replicas={replicas}")
                        writer.writerow([ts, tag, f"{rps:.2f}", f"{avg_lat:.1f}", replicas])
                        f.flush()
                    last_log = now

    print(f"✅ Test complete. Results saved to {CSV_FILE}")

if __name__ == "__main__":
    asyncio.run(main())