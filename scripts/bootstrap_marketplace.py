"""Pin the restored local SFTP public key via trusted Docker access; never reset credentials."""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    fingerprint = subprocess.check_output(
        [
            "docker",
            "exec",
            "sftp",
            "ssh-keygen",
            "-lf",
            "/etc/ssh/ssh_host_ed25519_key.pub",
            "-E",
            "sha256",
        ],
        text=True,
    ).split()[1]
    path = ROOT / ".env"
    contents = path.read_text(encoding="utf-8")
    existing = dict(
        line.split("=", 1)
        for line in contents.splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    )
    previous = existing.get("SHOPVN_SFTP_HOST_KEY_SHA256")
    if previous and previous != fingerprint:
        raise ValueError("Existing pin differs; review source replacement before explicit rotation")
    if not previous:
        with path.open("a", encoding="utf-8") as stream:
            stream.write("\nSHOPVN_SFTP_HOST_KEY_SHA256=" + fingerprint + "\n")
    print("Local SFTP public key pinned; source/credentials unchanged")


if __name__ == "__main__":
    main()
