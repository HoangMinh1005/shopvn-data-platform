"""Render a Finance chart from Trino query results, never from hardcoded example numbers."""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

ROOT = Path(__file__).resolve().parents[1]


def main():
    report = json.loads((ROOT / "runtime/direct/direct-slice-report.json").read_text())
    if report["status"] != "passed":
        raise RuntimeError("Only render certified results after reconciliation passes")
    rows = json.loads((ROOT / "runtime/direct/fin02-trino.json").read_text())["rows"]
    plt.rcParams["svg.hashsalt"] = "shopvn-fin02"
    figure, axis = plt.subplots(figsize=(12, 5.5))
    for channel, color in (("web", "#2563eb"), ("app", "#f97316")):
        records = sorted((row for row in rows if row[1] == channel), key=lambda row: row[0])
        axis.plot([date.fromisoformat(row[0]) for row in records],
                  [float(Decimal(row[3]) / Decimal(1_000_000_000)) for row in records],
                  label=channel, color=color, linewidth=2, marker="o", markersize=3)
    axis.set(title="Direct Finance — Net customer amount after refund (FIN02)",
             xlabel="Order date — June 2026", ylabel="Billion VND")
    axis.xaxis.set_major_formatter(DateFormatter("%d/%m"))
    axis.grid(alpha=0.2)
    axis.legend(title="Channel")
    figure.text(0.06, 0.02, "Shipping included. Order-date cohorts restated from observed source status; not bank cash by day.\nSource = Spark = Trino reconciliation passed; same input replayed in 3 fresh Spark processes.", fontsize=8, color="#475569")
    figure.tight_layout(rect=(0, 0.09, 1, 1))
    output = ROOT / "docs/verification/images"
    output.mkdir(parents=True, exist_ok=True)
    svg_path = output / "direct-fin02.svg"
    figure.savefig(svg_path, metadata={"Date": None})
    # Matplotlib emits trailing spaces in path data; normalize text without changing geometry.
    svg_path.write_text("\n".join(line.rstrip() for line in svg_path.read_text(encoding="utf-8").splitlines()) + "\n", encoding="utf-8")
    figure.savefig(ROOT / "runtime/direct/direct-fin02.png", dpi=150)
    plt.close(figure)
    print("Chart rendered from verified Trino results")


if __name__ == "__main__":
    main()
