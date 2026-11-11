"""Spark-submit CLI: orchestration delegates business logic to the pipeline."""

import argparse


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--candidate-name")
    parser.add_argument("--result-path")
    parser.add_argument("--publication-id")
    args = parser.parse_args(argv)
    from shopvn.pipelines.direct_finance import run

    run(
        args.snapshot_id,
        candidate_name=args.candidate_name,
        result_path=args.result_path,
        publication_id=args.publication_id,
    )


if __name__ == "__main__":
    main()
