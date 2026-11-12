"""Read-only pinned-host SFTP capture, verified immutable artifacts and independent baseline."""

import argparse
import base64
import csv
import hashlib
import json
import os
import re
import socket
import time
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal

import paramiko

from shopvn.common.environment import load_env
from shopvn.config import ProjectPaths
from shopvn.contracts.marketplace import COLUMNS, PARTNERS, file_name, parse_row, selection


@contextmanager
def connection():
    expected = os.environ.get("SHOPVN_SFTP_HOST_KEY_SHA256")
    if not expected or expected.startswith("replace-"):
        raise ValueError("Pin SHOPVN_SFTP_HOST_KEY_SHA256 before SFTP authentication")
    sock = socket.create_connection(
        (
            os.getenv("SHOPVN_SFTP_HOST", "127.0.0.1"),
            int(os.getenv("SHOPVN_SFTP_HOST_PORT", "2222")),
        ),
        timeout=15,
    )
    transport = paramiko.Transport(sock)
    try:
        transport.start_client(timeout=15)
        observed = "SHA256:" + base64.b64encode(
            hashlib.sha256(transport.get_remote_server_key().asbytes()).digest()
        ).decode().rstrip("=")
        if observed != expected:
            raise ValueError("SFTP host key mismatch; authentication refused")
        transport.auth_password(os.environ["SHOPVN_SFTP_USER"], os.environ["SHOPVN_SFTP_PASSWORD"])
        with paramiko.SFTPClient.from_transport(transport) as client:
            client.get_channel().settimeout(30)
            yield client
    finally:
        transport.close()


def parse_file(path, partner, day):
    """Independent stdlib/Decimal source reference; no Spark formulas used here."""
    seen, orders = {}, set()
    result = {
        "rows": 0,
        "accepted": 0,
        "rejected": 0,
        "duplicates": 0,
        "eligible_orders": 0,
        "units": 0,
        "merchandise": Decimal(0),
        "reported_net": Decimal(0),
        "formula_warnings": 0,
        "blocking_errors": 0,
        "status_counts": {},
    }
    with (
        path.open(encoding="utf-8-sig", newline="") as stream,
        path.with_suffix(".quarantine.jsonl").open("w", encoding="utf-8") as quarantine,
    ):
        reader = csv.DictReader(stream, strict=True)
        if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames):
            raise ValueError("Missing/duplicate CSV columns")
        if not set(COLUMNS) <= set(reader.fieldnames):
            raise ValueError("Missing required CSV columns")
        result["ignored_columns"] = sorted(set(reader.fieldnames) - set(COLUMNS))
        for row in reader:
            result["rows"] += 1
            try:
                identity, digest, merchandise, net, delta = parse_row(row, partner, day)
                if identity in seen:
                    if seen[identity] != digest:
                        raise ValueError("Conflicting duplicate identity")
                    result["duplicates"] += 1
                    result["rejected"] += 1
                    quarantine.write(
                        json.dumps({"row": reader.line_num, "reason": "duplicate_same_payload"})
                        + "\n"
                    )
                    continue
                seen[identity] = digest
                result["accepted"] += 1
                result["status_counts"][row["status"]] = (
                    result["status_counts"].get(row["status"], 0) + 1
                )
                result["formula_warnings"] += int(delta != 0)
                if row["status"] == "completed":
                    result["units"] += int(row["quantity_sold"])
                    result["merchandise"] += merchandise
                    result["reported_net"] += net
                    orders.add(row["external_order_id"])
            except ValueError as exc:
                result["rejected"] += 1
                result["blocking_errors"] += 1
                quarantine.write(json.dumps({"row": reader.line_num, "reason": str(exc)}) + "\n")
    result["eligible_orders"] = len(orders)
    return result


def capture_file(client, root, partner, day):
    name = file_name(partner, day)
    remote = f"/marketplace/incoming/{partner}/{name}"
    record = {
        "partner": partner,
        "business_date": day,
        "filename": name,
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        with client.open(remote + ".md5", "rb") as stream:
            expected = stream.read(4096).decode().strip().split()[0].lower()
        if not re.fullmatch(r"[a-f0-9]{32}", expected):
            raise ValueError("Invalid MD5 format")
    except FileNotFoundError:
        try:
            client.stat(remote)
            return {**record, "state": "checksum_missing"}
        except FileNotFoundError:
            return {**record, "state": "missing"}
    except (ValueError, IndexError, UnicodeError):
        return {**record, "state": "checksum_invalid"}
    path = root / name
    partial = root / (name + ".partial")
    md5, sha = hashlib.md5(), hashlib.sha256()
    try:
        with client.open(remote, "rb") as source, partial.open("wb") as target:
            while chunk := source.read(1024 * 1024):
                md5.update(chunk)
                sha.update(chunk)
                target.write(chunk)
    except FileNotFoundError:
        return {**record, "state": "missing"}
    record.update(
        md5=md5.hexdigest(),
        expected_md5=expected,
        sha256=sha.hexdigest(),
        bytes=partial.stat().st_size,
    )
    if md5.hexdigest() != expected:
        return {**record, "state": "checksum_mismatch"}
    partial.replace(path)
    try:
        reference = parse_file(path, partner, day)
    except (ValueError, csv.Error, UnicodeError, IndexError):
        return {**record, "state": "invalid_schema_or_payload"}
    return {
        **record,
        "state": "invalid_schema_or_payload" if reference["blocking_errors"] else "verified",
        "reference": reference,
    }


def extract(start, end, partners=PARTNERS, *, paths=None, client=None, wait_seconds=0):
    paths = paths or ProjectPaths.from_env()
    load_env(paths.project_root / ".env")
    scopes = selection(start, end, partners)
    if not 0 <= wait_seconds <= 60:
        raise ValueError("Arrival wait must be bounded between 0 and 60 seconds")
    capture_id = uuid.uuid4().hex
    root = paths.runtime / "marketplace/landing" / capture_id
    root.mkdir(parents=True)
    manifest = {
        "capture_id": capture_id,
        "contract_version": 1,
        "rule_version": "1.1",
        "data_origin": "source",
        "start": start,
        "end": end,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "files": [],
    }

    def read_files(source):
        for partner, day in scopes:
            entry = capture_file(source, root, partner, day)
            entry["capture_id"] = capture_id
            manifest["files"].append(entry)
        deadline = time.monotonic() + wait_seconds
        while wait_seconds and time.monotonic() < deadline:
            missing = [
                i
                for i, row in enumerate(manifest["files"])
                if row["state"] in {"missing", "checksum_missing"}
            ]
            if not missing:
                break
            time.sleep(min(5, max(0, deadline - time.monotonic())))
            for index in missing:
                previous = manifest["files"][index]
                entry = capture_file(source, root, previous["partner"], previous["business_date"])
                manifest["files"][index] = {**entry, "capture_id": capture_id}

    if client is None:
        with connection() as source:
            read_files(source)
    else:
        read_files(client)
    manifest["states"] = dict(Counter(row["state"] for row in manifest["files"]))
    manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
    manifest["arrival_wait_seconds"] = wait_seconds
    destination = root / "manifest.json"
    destination.write_text(json.dumps(manifest, default=str, indent=2), encoding="utf-8")
    print("Marketplace capture: " + json.dumps(manifest["states"]), flush=True)
    return destination


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--partners", default=",".join(PARTNERS))
    args = parser.parse_args()
    print(extract(args.start, args.end, args.partners.split(",")))


if __name__ == "__main__":
    main()
