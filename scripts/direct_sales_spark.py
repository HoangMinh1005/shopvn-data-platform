"""Spark-submit entry point for the direct Finance slice."""

import argparse

from shopvn.transformations.direct_sales import run

parser = argparse.ArgumentParser()
parser.add_argument("--snapshot-id", required=True)
parser.add_argument("--candidate-name")
parser.add_argument("--result-path")
parser.add_argument("--publication-id")
args = parser.parse_args()
run(args.snapshot_id, candidate_name=args.candidate_name, result_path=args.result_path, publication_id=args.publication_id)
