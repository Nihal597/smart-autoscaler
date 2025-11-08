FROM python:3.11-slim
WORKDIR /app
COPY controller.py /app/controller.py
RUN pip install --no-cache-dir kubernetes requests prometheus_client
ENV PROMETHEUS_URL="http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090"
CMD ["python", "/app/controller.py"]