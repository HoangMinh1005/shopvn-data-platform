"""Download pinned upstream build artifacts into ignored runtime storage, with retries."""

import argparse
import hashlib
import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = {
    "iceberg-spark-runtime-3.5_2.12-1.12.0.jar": "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.12.0/iceberg-spark-runtime-3.5_2.12-1.12.0.jar",
    "iceberg-aws-bundle-1.12.0.jar": "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar",
    "postgresql-42.7.8.jar": "https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar",
    "minio-source.tar.gz": "https://codeload.github.com/minio/minio/tar.gz/9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a",
}


def download_maven(url: str, destination: Path):
    """Bound requests to 128 KiB; cached parts allow retry on a slow connection."""
    request = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(request, timeout=30) as response:
        size = int(response.headers["Content-Length"])
    chunk_size = 128 * 1024
    part_folder = destination.parent / (destination.name + ".parts")
    part_folder.mkdir(exist_ok=True)

    def fetch(start: int) -> Path:
        end = min(start + chunk_size, size) - 1
        part = part_folder / f"{start:012d}"
        if part.exists() and part.stat().st_size == end - start + 1:
            return part
        for attempt in range(4):
            try:
                request = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}"})
                with urllib.request.urlopen(request, timeout=30) as response:
                    if response.status != 206:
                        raise ValueError("Range request not supported")
                    content = response.read(end - start + 2)
                if len(content) != end - start + 1:
                    raise ValueError("Truncated range")
                part.write_bytes(content)
                return part
            except (OSError, ValueError):
                if attempt == 3:
                    raise
                time.sleep(1 + attempt)
        raise AssertionError("Unreachable")

    with ThreadPoolExecutor(max_workers=16) as executor, destination.open("wb") as output:
        for index, part in enumerate(executor.map(fetch, range(0, size, chunk_size))):
            output.write(part.read_bytes())
            if index % 64 == 0:
                print(f"{destination.name}: {min((index + 1) * chunk_size, size)}/{size} bytes", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=list(ARTIFACTS), action="append")
    args = parser.parse_args()
    target = ROOT / "runtime/spike-build"
    target.mkdir(parents=True, exist_ok=True)
    hashes = {}
    for filename, url in ARTIFACTS.items():
        if args.only and filename not in args.only:
            continue
        destination = target / filename
        checksum = None
        checksum_algorithm = None
        if filename.endswith(".jar"):
            for algorithm in ("sha512", "sha256", "sha1"):
                try:
                    with urllib.request.urlopen(url + "." + algorithm, timeout=60) as response:
                        checksum = response.read().decode().strip().split()[0].lower()
                        checksum_algorithm = algorithm
                        break
                except urllib.error.HTTPError as exc:
                    if exc.code != 404:
                        raise
            if checksum is None:
                raise ValueError(f"No upstream checksum published for {filename}")
        if destination.exists() and checksum:
            with destination.open("rb") as stream:
                if hashlib.file_digest(stream, checksum_algorithm).hexdigest() == checksum:
                    print(f"Verified cached {filename}", flush=True)
                    hashes[filename] = {"sha256": hashlib.sha256(destination.read_bytes()).hexdigest(), "url": url}
                    continue
        for attempt in range(5):
            try:
                print(f"Downloading {filename}, attempt {attempt + 1}", flush=True)
                count = 0
                if filename.endswith(".jar"):
                    download_maven(url, destination)
                else:
                    with urllib.request.urlopen(url, timeout=60) as response, destination.open("wb") as output:
                        expected_length = int(response.headers.get("Content-Length", 0))
                        while chunk := response.read(64 * 1024):
                            output.write(chunk)
                            count += len(chunk)
                    if expected_length and count != expected_length:
                        raise ValueError("Truncated artifact")
                if checksum:
                    with destination.open("rb") as stream:
                        if hashlib.file_digest(stream, checksum_algorithm).hexdigest() != checksum:
                            raise ValueError("Upstream artifact checksum mismatch")
                break
            except (OSError, ValueError):
                if attempt == 4:
                    raise
                time.sleep(min(2 ** attempt, 15))
        with destination.open("rb") as stream:
            hashes[filename] = {"sha256": hashlib.file_digest(stream, "sha256").hexdigest(), "url": url}
        print(f"Verified {filename}", flush=True)
    # Per-artifact receipts permit independent downloads without manifest write races.
    for filename, receipt in hashes.items():
        (target / (filename + ".receipt.json")).write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    if not args.only:
        (target / "artifacts.json").write_text(json.dumps(hashes, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
