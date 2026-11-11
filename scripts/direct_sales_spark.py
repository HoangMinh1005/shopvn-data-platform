"""Spark-submit entry point for the direct Finance slice."""

import argparse

from shopvn.transformations.direct_sales import run

parser = argparse.ArgumentParser()
parser.add_argument("--snapshot-id", required=True)
args = parser.parse_args()
run(args.snapshot_id)
