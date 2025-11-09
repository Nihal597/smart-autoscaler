# ---------------- analyze_results.py ----------------
import pandas as pd, matplotlib.pyplot as plt, numpy as np

df = pd.read_csv("load_test_results.csv")
df = df.dropna(subset=["latency_s"])
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
df["latency_s"] = df["latency_s"].astype(float)

# --- Compute overall summary ---
summary = df.groupby("url")["latency_s"].agg(["count","mean","max","min"])
print("\n=== Overall Summary (seconds) ===")
print(summary)
print("\nAverage latency (ms):")
print(df.groupby("url")["latency_s"].mean()*1000)

# --- Plot latency over time ---
plt.figure(figsize=(10,5))
for url,g in df.groupby("url"):
    plt.plot(g["timestamp"], g["latency_s"]*1000, ".", ms=2, label=url)
plt.title("Latency over Time (Smart vs HPA)")
plt.ylabel("Latency (ms)")
plt.xlabel("Time")
plt.legend()
plt.tight_layout()
plt.savefig("latency_over_time.png")

# --- Aggregate by phase ---
phase_stats = df.groupby(["phase","url"])["latency_s"].agg(["mean","count"])
phase_stats["mean_ms"] = phase_stats["mean"]*1000
print("\n=== Phase-wise average latency (ms) ===")
print(phase_stats[["mean_ms"]])

# --- Throughput estimation ---
window = "10s"
df.set_index("timestamp", inplace=True)
rps = df.groupby("url").resample(window).size().unstack(0).fillna(0)
rps.plot(figsize=(10,4))
plt.title("Throughput (Requests per 10 s)")
plt.ylabel("Requests")
plt.tight_layout()
plt.savefig("throughput.png")

plt.show()
