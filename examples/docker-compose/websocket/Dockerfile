FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY pyproject.toml README.md LICENSE ./
COPY localqueue ./localqueue

RUN pip install --no-cache-dir ".[cli]" websockets>=15.0.1

USER root
