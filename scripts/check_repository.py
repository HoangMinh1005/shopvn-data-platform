"""Validate repository layout and maintained documentation without third-party packages."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_PATHS = (
    "AGENTS.md",
    ".env.example",
    "README.md",
    "CONTRIBUTING.md",
    "docs/sources/01_project_requirements.md",
    "docs/sources/02_dataset_guide.md",
    "docs/sources/03_design_doc_template.md",
    "docs/sources/images/ERD.png",
    "docs/sources/images/datasources.png",
    "docs/business/business-rules-v1.md",
    "docs/architecture/baseline-v1.md",
    "docs/architecture/mvp-implementation-plan.md",
    "docs/data-model/README.md",
    "infra/sources/docker-compose.yml",
    "infra/platform/README.md",
    "dags",
    "src/shopvn",
    "schemas",
    "sql",
    "tests/fixtures",
)
MARKDOWN_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")


def run_git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(ROOT), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def main() -> int:
    errors: list[str] = []
    for relative in REQUIRED_PATHS:
        if not (ROOT / relative).exists():
            errors.append(f"Missing required path: {relative}")

    documents = [ROOT / "README.md", ROOT / "CONTRIBUTING.md"]
    for folder in ("docs", "infra", "dags", "src", "schemas", "sql", "scripts", "tests"):
        for document in (ROOT / folder).rglob("*.md"):
            # Preserve course documents, including unfinished template image links.
            relative = document.relative_to(ROOT)
            if relative.parts[:2] == ("docs", "sources"):
                continue
            documents.append(document)

    for document in documents:
        for match in MARKDOWN_LINK.finditer(document.read_text(encoding="utf-8")):
            target = match.group(1).strip().strip("<>")
            parsed = urlsplit(target)
            if parsed.scheme or not parsed.path:
                continue
            destination = document.parent / unquote(parsed.path)
            if not destination.exists():
                errors.append(f"Broken link in {document.relative_to(ROOT)}: {target}")

    tracked = run_git("ls-files", "-z")
    if tracked.returncode:
        errors.append("Cannot inspect Git index; run inside an initialized Git repository.")
    else:
        for name in filter(None, tracked.stdout.split("\0")):
            path = Path(name)
            sensitive_env = path.name.startswith(".env") and path.name != ".env.example"
            if sensitive_env or path.suffix in {".pem", ".key", ".p12", ".dump"}:
                errors.append(f"Sensitive configuration/file tracked by Git: {name}")
            if path.parts[0] in {"data", "runtime", "logs", "checkpoints", "volumes", "tmp"}:
                errors.append(f"Runtime/source data tracked by Git: {name}")

    for ignored in (".env", "data/probe.csv", "logs/probe.log", "checkpoints/probe"):
        if run_git("check-ignore", "-q", "--", ignored).returncode != 0:
            errors.append(f"Expected Git ignore rule missing: {ignored}")
    for allowed in (".env.example", "tests/fixtures/example.csv"):
        if run_git("check-ignore", "-q", "--", allowed).returncode == 0:
            errors.append(f"Expected trackable file is ignored: {allowed}")

    if errors:
        print("Repository checks failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print(f"Repository checks passed: layout, {len(documents)} documents, Git index and ignore rules.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
