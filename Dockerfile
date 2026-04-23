FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN addgroup --system localqueue \
    && adduser --system --ingroup localqueue --home /app localqueue

COPY pyproject.toml README.md LICENSE ./
COPY localqueue ./localqueue

RUN pip install --no-cache-dir ".[cli]"

USER localqueue

ENTRYPOINT ["localqueue"]
CMD ["--help"]
