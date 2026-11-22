#!/bin/sh
# Create or update every connector in /connectors. File name (minus .json) is the connector name.
set -eu
for f in /connectors/*.json; do
  name=$(basename "$f" .json)
  code=$(curl -s -o /tmp/out -w '%{http_code}' -X PUT -H 'Content-Type: application/json' \
    --data @"$f" "http://kafka-connect:8083/connectors/${name}/config")
  echo "[connect-init] ${name}: HTTP ${code}"
  case "$code" in 200|201) ;; *) cat /tmp/out; exit 1;; esac
done
