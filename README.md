## Step-by-Step Guide: Kubernetes Cluster Setup with Minikube, Prometheus, and Grafana using `values.yml`

This guide walks you through setting up a Kubernetes cluster using Minikube, and deploying Prometheus and Grafana in the `monitoring` namespace using a custom `values.yml` file.

### Prerequisites

- [Minikube](https://minikube.sigs.k8s.io/docs/) installed
- [kubectl](https://kubernetes.io/docs/tasks/tools/) installed
- [Helm](https://helm.sh/docs/intro/install/) installed
- A `values.yml` file configured for Prometheus and Grafana

---

### 1. Start Minikube

```sh
minikube start
```

---

### 2. Create the `monitoring` Namespace

```sh
kubectl create namespace monitoring
```

---

### 3. Add Helm Repositories

```sh
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
```

---

### 4. Deploy Prometheus using `values.yml`

```sh
helm install prometheus prometheus-community/kube-prometheus-stack -n monitoring -f values.yml
```

### 6. Verify Deployments

```sh
kubectl get pods -n monitoring
```

---

### 7. Access Grafana Dashboard

```sh
kubectl port-forward svc/grafana 3000:80 -n monitoring
```
Open [http://localhost:3000](http://localhost:3000) in your browser.

---

### 8. Access Prometheus Dashboard

```sh
kubectl port-forward svc/prometheus-server 9090:80 -n monitoring
```
Open [http://localhost:9090](http://localhost:9090) in your browser.

---
