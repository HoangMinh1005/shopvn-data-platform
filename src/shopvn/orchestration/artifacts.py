"""Shared run artifacts: standard library only, usable from the isolated Spark Python."""

import json
from pathlib import Path

RUNTIME = Path("/workspace/runtime/orchestration/runs")


def folder(key):
    if len(key) != 24 or any(c not in "abcdef0123456789" for c in key):
        raise ValueError("Invalid run key")
    path = RUNTIME / key
    path.mkdir(parents=True, exist_ok=True)
    return path


def save(key, name, value):
    (folder(key) / name).write_text(json.dumps(value, default=str, indent=2), encoding="utf-8")


def load(key, name):
    return json.loads((folder(key) / name).read_text(encoding="utf-8"))
