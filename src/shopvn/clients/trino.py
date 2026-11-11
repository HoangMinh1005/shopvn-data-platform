"""Minimal HTTP Trino client used by checks and orchestration."""

import json
import os
import time
import urllib.request


def trino_query(statement: str) -> list:
    request = urllib.request.Request(
        os.getenv("TRINO_URL", "http://127.0.0.1:18080").rstrip("/") + "/v1/statement",
        data=statement.encode(),
        headers={"X-Trino-User": "readiness", "Content-Type": "text/plain"},
    )
    result = []
    deadline = time.monotonic() + 180
    while True:
        if time.monotonic() > deadline:
            raise TimeoutError("Trino query timeout")
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
        if "error" in payload:
            raise RuntimeError(payload["error"]["message"])
        result.extend(payload.get("data", []))
        if "nextUri" not in payload:
            return result
        request = urllib.request.Request(payload["nextUri"], headers={"X-Trino-User": "readiness"})
        time.sleep(0.2)
