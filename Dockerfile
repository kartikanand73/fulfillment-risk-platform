# Refresh job image: deterministic Python only (no model SDKs). Runs as a non-root user.
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY fulfillment_risk ./fulfillment_risk
RUN pip install --no-cache-dir ".[azure]" && useradd -u 10001 -m fr
USER 10001
ENTRYPOINT ["python", "-m", "fulfillment_risk"]
CMD ["refresh", "--live", "--trigger", "schedule"]
