# llm-gateway: stdlib-only, no pip, no build step.
FROM python:3.13-slim
RUN useradd -m -u 1000 gw
COPY gateway.py /app/gateway.py
USER gw
EXPOSE 4143
ENV GATEWAY_CONFIG=/config/config.json GATEWAY_DB=/data/usage.db
VOLUME ["/config", "/data"]
HEALTHCHECK --interval=30s --timeout=5s CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:4143/healthz', timeout=4)"
CMD ["python3", "/app/gateway.py"]
