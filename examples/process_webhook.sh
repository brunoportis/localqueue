#!/bin/sh
set -eu

payload="$(cat)"
url="$(printf '%s' "$payload" | jq -r '.url')"
body="$(printf '%s' "$payload" | jq -c '.payload')"

curl -fsS -X POST "$url" \
  -H 'content-type: application/json' \
  -d "$body"
