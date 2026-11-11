"""Runtime paths shared by package jobs, local tools and containers."""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    project_root: Path
    runtime: Path

    @classmethod
    def from_env(cls):
        source_root = Path(__file__).resolve().parents[2]
        default_root = source_root if (source_root / "src/shopvn").is_dir() else Path.cwd()
        root = Path(os.getenv("SHOPVN_PROJECT_ROOT", default_root)).resolve()
        runtime = Path(os.getenv("SHOPVN_RUNTIME_DIR", "runtime"))
        return cls(root, (root / runtime).resolve())

    @property
    def direct(self):
        return self.runtime / "direct"

    @property
    def landing(self):
        return self.direct / "landing"

    @property
    def hmac_key(self):
        return self.direct / "hmac.key"

    def result_file(self, requested=None):
        path = Path(requested).resolve() if requested else self.direct / "direct-result.json"
        if not path.is_relative_to(self.runtime):
            raise ValueError("Result path must stay inside runtime")
        return path
