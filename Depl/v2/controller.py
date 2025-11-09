#!/usr/bin/env python3
# controller.py - Smart Autoscaler (EWMA + PID)
import os
import time
import math
import requests
from kubernetes import client, config
from prometheus_client import start_http_server, Counter, Gauge

# Controller exported metrics
desired_replicas_g = Gauge("smartscaler_desired_replicas", "Desired replicas")
current_replicas_g = Gauge("smartscaler_current_replicas", "Current replicas")
combined_g = Gauge("smartscaler_combined", "Combined metric value")
pid_g = Gauge("smartscaler_pid_value", "PID controller output")
ewma_g = Gauge("smartscaler_ewma_value", "EWMA smoothed load value")
scale_actions_counter = Counter(
    "smartscaler_scale_actions_total",
    "Total number of scaling actions performed by Smart Autoscaler",
    ["direction"],
)

# Configuration from env (override as needed)
PROM_URL = os.getenv(
    "PROMETHEUS_URL",
    "http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090",
)
NAMESPACE = os.getenv("NAMESPACE", "default")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "15"))

EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.25"))
KP = float(os.getenv("KP", "1.2"))
KI = float(os.getenv("KI", "0.12"))
KD = float(os.getenv("KD", "0.02"))

MIN_REPLICAS = int(os.getenv("MIN_REPLICAS", "1"))
MAX_REPLICAS = int(os.getenv("MAX_REPLICAS", "5"))
TARGET_DEPLOYMENT_ENV = os.getenv(
    "TARGET_DEPLOYMENT", "integration-svc-depl-autoscaler"
)  # name of deployment to scale
CR_NAME = os.getenv("SMARTSCALER_CR", "integration-scaler")

# Amplification factor for raw_combined (tuneable; 1.0 = no amplify)
COMBINED_AMPLIFY = float(os.getenv("COMBINED_AMPLIFY", "1.0"))

_last_metric_cache = {}


class EWMA:
    def __init__(self, alpha):
        self.alpha = alpha
        self.value = None

    def update(self, x):
        x = float(x)
        if self.value is None:
            self.value = x
        else:
            self.value = self.alpha * x + (1 - self.alpha) * self.value
        return self.value


class PID:
    def __init__(self, kp, ki, kd):
        self.kp = kp
        self.ki = ki
        self.kd = kd
        self.prev_err = 0.0
        self.integral = 0.0
        self.last_output = 0.0
        # anti-windup limits
        self.integral_min = -10.0
        self.integral_max = 10.0

    def update(self, err, dt):
        # integrate with anti-windup
        self.integral += err * dt
        self.integral = max(self.integral_min, min(self.integral_max, self.integral))
        deriv = (err - self.prev_err) / dt if dt > 0 else 0.0
        out = self.kp * err + self.ki * self.integral + self.kd * deriv
        self.prev_err = err
        self.last_output = out
        return out


def prom_query(q):
    """Query Prometheus and return a single scalar (average of series) - caches on error."""
    try:
        r = requests.get(f"{PROM_URL}/api/v1/query", params={"query": q}, timeout=8)
        r.raise_for_status()
        data = r.json().get("data", {}).get("result", [])
        if not data:
            val = 0.0
        else:
            vals = [float(item["value"][1]) for item in data]
            # average across returned series
            val = sum(vals) / len(vals)
        _last_metric_cache[q] = val
        return val
    except Exception as e:
        print("[prom_query] error:", e)
        # return last known value if present; else 0.0
        return _last_metric_cache.get(q, 0.0)


def scale_deployment(apps, name, replicas):
    try:
        body = {"spec": {"replicas": replicas}}
        apps.patch_namespaced_deployment_scale(name=name, namespace=NAMESPACE, body=body)
        print(f"[scale] {name} -> {replicas}")
    except Exception as e:
        print("[scale] patch failed:", e)


def main():
    # kube config
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()

    apps = client.AppsV1Api()
    custom_api = client.CustomObjectsApi()

    # METRICS config (promql must match the labels you observed in Prometheus)
    METRICS = [
        {
            "name": "cpu",
            "promql": f'''
                avg by (pod) (
                    rate(container_cpu_usage_seconds_total{{namespace="{NAMESPACE}",
                        pod=~"integration-svc-depl-autoscaler-.*",
                        container!="POD"
                    }}[1m])
                )
            ''',
            "target": 0.25,  # cores per pod threshold (tune)
            "weight": 0.6,
        },
        {
            "name": "rps",
            "promql": f'''
                sum by (pod) (
                    rate(http_server_requests_seconds_count{{
                        namespace="{NAMESPACE}",
                        application="integration-svc-smart",
                        status="200",
                        uri="/api/v1/monitor/{{number}}"
                    }}[1m])
                )
            ''',
            "target": 0.05,  # realistic target for your slow Fibonacci endpoint
            "weight": 0.4,
        },
    ]

    ewmas = {m["name"]: EWMA(EWMA_ALPHA) for m in METRICS}
    pids = {m["name"]: PID(KP, KI, KD) for m in METRICS}

    print("[smartscaler] starting; poll_interval", POLL_INTERVAL)
    last_time = time.time()

    while True:
        start_loop = time.time()
        weighted_sum = 0.0
        total_weight = 0.0

        # Gather metrics and compute per-metric PID and EWMA
        for m in METRICS:
            raw = prom_query(m["promql"])
            smooth = ewmas[m["name"]].update(raw)
            # avoid division by zero target
            err = 0.0 if m["target"] == 0 else (smooth - m["target"]) / float(m["target"])
            pid_out = pids[m["name"]].update(err, POLL_INTERVAL)
            # clamp PID output to allow range for small workloads (wider than before)
            pid_out = max(-3.0, min(3.0, pid_out))
            weighted_sum += pid_out * m["weight"]
            total_weight += m["weight"]
            print(
                f"[metric] {m['name']} raw={raw:.4f} smooth={smooth:.4f} "
                f"err={err:.4f} pid={pid_out:.4f}"
            )

        # Compute combined signal (normalize by total_weight, amplify with COMBINED_AMPLIFY)
        raw_combined = (weighted_sum / (total_weight or 1.0)) * COMBINED_AMPLIFY

        # soft clamp to avoid hard saturation but still allow excursions
        def soft_clamp(x, low=-1.0, high=1.0, softness=0.25):
            if x < low:
                return low + (x - low) * softness
            elif x > high:
                return high + (x - high) * softness
            return x

        combined = soft_clamp(raw_combined)
        print(f"[decision] combined={combined:.4f} raw_combined={raw_combined:.4f}")

        # Dynamic threshold adjustment via CR (if present)
        try:
            cr = custom_api.get_namespaced_custom_object(
                group="autoscale.monitoring.io",
                version="v1",
                namespace=NAMESPACE,
                plural="smartscalers",
                name=CR_NAME,
            )
            dt = cr.get("spec", {}).get("dynamicThreshold", {}) or {}
            if dt.get("enabled"):
                af = float(dt.get("adjustmentFactor", 0.0))
                if af != 0.0:
                    combined = combined * (1.0 + af) if combined > 0 else combined * (1.0 - af)
                    print(f"[dynamic] CR={CR_NAME} adjustmentFactor={af:.3f} combined_adjusted={combined:.4f}")
        except Exception as e:
            # not fatal; CR may not exist
            pass

        # Read deployment to determine current replicas
        try:
            dep = apps.read_namespaced_deployment(TARGET_DEPLOYMENT_ENV, NAMESPACE)
            current = int(dep.status.replicas or dep.spec.replicas or 1)
        except Exception as e:
            print("[main] read deployment failed:", e)
            time.sleep(POLL_INTERVAL)
            continue

        # Translate combined signal to desired replica change
        raw_factor = current * (1.0 + combined)
        raw_desired = math.ceil(raw_factor) if combined >= 0 else math.floor(raw_factor)
        raw_desired = max(0, raw_desired)
        delta = raw_desired - current

        if delta > 0:
            desired = current + 1
        elif delta < 0:
            desired = current - 1
        else:
            desired = raw_desired

        desired = max(MIN_REPLICAS, min(MAX_REPLICAS, desired))

        # Update Prometheus client metrics (use last_output for pid)
        try:
            avg_pid = sum((pids[m["name"]].last_output for m in METRICS)) / len(METRICS)
            avg_ewma = sum((ewmas[m["name"]].value or 0.0 for m in METRICS)) / len(METRICS)
            pid_g.set(float(avg_pid))
            ewma_g.set(float(avg_ewma))
            combined_g.set(float(combined))
            desired_replicas_g.set(int(desired))
            current_replicas_g.set(int(current))
        except Exception as e:
            print("[metrics] PID/EWMA update failed:", e)

        # Apply scaling if necessary
        if desired != current:
            direction = "up" if desired > current else "down"
            scale_actions_counter.labels(direction=direction).inc()
            print(f"[scale] current={current} desired={desired} combined={combined:.4f}")
            scale_deployment(apps, TARGET_DEPLOYMENT_ENV, desired)
        else:
            print(f"[stable] current={current} desired={desired} combined={combined:.4f}")

        # Sleep until next poll
        elapsed = time.time() - start_loop
        to_sleep = max(0, POLL_INTERVAL - elapsed)
        time.sleep(to_sleep)


if __name__ == "__main__":
    # start prometheus metrics endpoint (port 8000)
    start_http_server(8000)
    main()
