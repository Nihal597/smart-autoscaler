#!/usr/bin/env python3
# controller.py - Smart Autoscaler (EWMA + PID, minimal)
import os, time, math, requests
from kubernetes import client, config

from prometheus_client import start_http_server, Counter, Gauge


# controller Prometheus metrics
desired_replicas_g = Gauge("smartscaler_desired_replicas", "Desired replicas")
current_replicas_g = Gauge("smartscaler_current_replicas", "Current replicas")
combined_g = Gauge("smartscaler_combined", "Combined metric value")
pid_g = Gauge('smartscaler_pid_value', 'PID controller output')
ewma_g = Gauge('smartscaler_ewma_value', 'EWMA smoothed load value')

scale_actions_counter = Counter(
    'smartscaler_scale_actions_total',
    'Total number of scaling actions performed by Smart Autoscaler',
    ['direction']  # label for 'scale_up' or 'scale_down'
)

PROM_URL = os.getenv("PROMETHEUS_URL", "http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090")
NAMESPACE = os.getenv("NAMESPACE", "default")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "15"))

# PID & EWMA defaults
EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.25"))
KP, KI, KD = float(os.getenv("KP", "0.6")), float(os.getenv("KI", "0.015")), float(os.getenv("KD", "0.08"))

MIN_REPLICAS = int(os.getenv("MIN_REPLICAS", "1"))
MAX_REPLICAS = int(os.getenv("MAX_REPLICAS", "5"))
TARGET_DEPLOYMENT_ENV = os.getenv("TARGET_DEPLOYMENT", "integration-svc-depl")  # optional: can be defined via CR

class EWMA:
    def __init__(self, alpha): self.alpha, self.value = alpha, None
    def update(self, x):
        x = float(x)
        self.value = x if self.value is None else self.alpha * x + (1 - self.alpha) * self.value
        return self.value

class PID:
    def __init__(self, kp, ki, kd):
        self.kp, self.ki, self.kd = kp, ki, kd
        self.prev_err = 0.0
        self.integral = 0.0
        # anti-windup limits
        self.integral_min = -10.0
        self.integral_max = 10.0
    def update(self, err, dt):
        self.integral += err * dt
        # clamp integral (anti-windup)
        self.integral = max(self.integral_min, min(self.integral_max, self.integral))
        deriv = (err - self.prev_err) / dt if dt > 0 else 0.0
        out = self.kp * err + self.ki * self.integral + self.kd * deriv
        self.prev_err = err
        return out
    
_last_metric_cache = {}

def prom_query(q):
    try:
        r = requests.get(f"{PROM_URL}/api/v1/query", params={"query": q}, timeout=8)
        r.raise_for_status()
        data = r.json().get("data", {}).get("result", [])
        if not data:
            val = 0.0
        else:
            vals = [float(item["value"][1]) for item in data]
            # Use average across series so per-pod "avg by (pod)" queries are per-pod value
            val = sum(vals) / len(vals)
        _last_metric_cache[q] = val
        return val
    except Exception as e:
        print("[prom_query] error:", e)
        # return last known value if present to avoid forcing 0 on transient errors
        return _last_metric_cache.get(q, 0.0)

def scale_deployment(apps, name, replicas):
    try:
        body = {"spec": {"replicas": replicas}}
        apps.patch_namespaced_deployment_scale(name=name, namespace=NAMESPACE, body=body)
        print(f"[scale] {name} -> {replicas}")
    except Exception as e:
        print("[scale] patch failed:", e)

def main():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()

    apps = client.AppsV1Api()

    # CustomObjects API to read SmartScaler CR for dynamicThreshold settings
    custom_api = client.CustomObjectsApi()

    # Example: use CR-based metrics; fallback to env target if CR absent
    # Simple built-in metric set (used if you don't have a CR ready)
    METRICS = [
        # CPU: per‑pod cores (avg by pod). set target to cores per pod (start ~0.1-0.3)
        {"name": "cpu", "promql": f'avg by (pod) (rate(container_cpu_usage_seconds_total{{namespace="{NAMESPACE}", pod=~"integration-svc-.*"}}[1m]))', "target": 0.2, "weight": 0.5},
        # RPS: per‑pod requests/sec (avg). set target per‑pod (start ~0.02-0.2)
        {"name": "rps", "promql": f'avg by (pod) (rate(http_server_requests_seconds_count{{namespace="{NAMESPACE}", job="integration-svc"}}[1m]))', "target": 0.05, "weight": 0.3},
        # Queue (max per pod / cluster); tune target as needed
        {"name": "queue", "promql": f'max(integration_queue_length{{namespace="{NAMESPACE}", job="integration-svc"}})', "target": 5, "weight": 0.2}
    ]

    # per-metric EWMA and PID
    ewmas = {m["name"]: EWMA(EWMA_ALPHA) for m in METRICS}
    pids = {m["name"]: PID(KP, KI, KD) for m in METRICS}

    print("[smartscaler] starting; poll_interval", POLL_INTERVAL)
    while True:
        weighted_sum = 0.0
        total_weight = 0.0
        for m in METRICS:
            raw = prom_query(m["promql"])
            smooth = ewmas[m["name"]].update(raw)
            err = 0.0 if m["target"] == 0 else (smooth - m["target"]) / float(m["target"])
            pid_out = pids[m["name"]].update(err, POLL_INTERVAL)
            # clamp per-metric PID output to reasonable bounds to avoid extreme changes
            pid_out = max(-1.0, min(1.0, pid_out))
            weighted_sum += pid_out * m["weight"]
            total_weight += m["weight"]
            print(f"[metric] {m['name']} raw={raw:.3f} smooth={smooth:.3f} err={err:.3f} pid={pid_out:.4f}")

        combined = weighted_sum / (total_weight or 1.0)
        # clamp combined multiplier to avoid extreme jumps
        combined = max(-0.5, min(1.0, combined))

        # --- dynamic threshold adjustment (read SmartScaler CR) ---
        # If a SmartScaler CR exists and dynamicThreshold.enabled is true,
        # adjust the combined multiplier by the CR's adjustmentFactor.
        try:
            cr_name = os.getenv("SMARTSCALER_CR", "integration-scaler")
            cr = custom_api.get_namespaced_custom_object(
                group="autoscale.monitoring.io",
                version="v1",
                namespace=NAMESPACE,
                plural="smartscalers",
                name=cr_name,
            )
            dt = cr.get("spec", {}).get("dynamicThreshold", {}) or {}
            if dt.get("enabled"):
                af = float(dt.get("adjustmentFactor", 0.0))
                if af != 0.0:
                    # increase aggressiveness for scale-up, decrease for scale-down
                    combined = combined * (1.0 + af) if combined > 0 else combined * (1.0 - af)
                    print(f"[dynamic] CR={cr_name} adjustmentFactor={af:.3f} combined_adjusted={combined:.4f}")
        except Exception as e:
            # tolerate missing CR or transient API errors; keep combined as-is
            print("[dynamic] CR read failed or not present:", e)
        # --- end dynamic adjustment ---

        # find current replicas
        target_dep = TARGET_DEPLOYMENT_ENV or "integration-svc-depl"
        try:
            dep = apps.read_namespaced_deployment(target_dep, NAMESPACE)
            current = int(dep.status.replicas or dep.spec.replicas or 1)
        except Exception as e:
            print("[main] read deployment failed:", e)
            time.sleep(POLL_INTERVAL); continue
        
        raw_factor = current * (1.0 + combined)
        if combined >= 0:
            raw_desired = int(math.ceil(raw_factor))
        else:
            raw_desired = int(math.floor(raw_factor))
        raw_desired = max(0, raw_desired)
        # limit replica change per loop to +/-1 to make scaling gradual
        max_delta = 1
        delta = raw_desired - current
        if delta > max_delta:
            desired = current + max_delta
        elif delta < -max_delta:
            desired = current - max_delta
        else:
            desired = raw_desired

        # final clamp to configured bounds
        desired = max(MIN_REPLICAS, min(MAX_REPLICAS, desired))
        
        # expose latest average PID and EWMA across metrics for observability
        try:
            avg_pid = sum(pids[m["name"]].prev_err for m in METRICS) / len(METRICS)
            avg_ewma = sum(ewmas[m["name"]].value or 0.0 for m in METRICS) / len(METRICS)
            pid_g.set(float(avg_pid))
            ewma_g.set(float(avg_ewma))
        except Exception as e:
            print("[metrics] PID/EWMA update failed:", e)

        # update controller metrics for monitoring
        try:
            combined_g.set(float(combined))
            desired_replicas_g.set(int(desired))
            current_replicas_g.set(int(current))
        except Exception:
            pass

        if desired != current:
            direction = "up" if desired > current else "down"
            scale_actions_counter.labels(direction=direction).inc()
            scale_deployment(apps, target_dep, desired)
        else:
            print(f"[stable] current={current} desired={desired} combined={combined:.4f}")

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    # start prometheus metrics endpoint (port 8000)
    start_http_server(8000)
    main()
