FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY . .

RUN pip install --no-cache-dir ".[cli]"

ENTRYPOINT ["localqueue"]
CMD ["--help"]
