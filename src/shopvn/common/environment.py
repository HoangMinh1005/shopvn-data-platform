"""Optional local .env loading; explicit process environment always wins."""

import os
from pathlib import Path

from shopvn.config import ProjectPaths


def load_env(path: Path | None = None):
    path = path or ProjectPaths.from_env().project_root / ".env"
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.lstrip().startswith("#") and "=" in line:
            name, value = line.split("=", 1)
            os.environ.setdefault(name.strip(), value.strip())
