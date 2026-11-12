"""Spark-submit entry point for an actual inventory observation."""

import argparse


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--mode", choices=("current", "eod"), default="current")
    parser.add_argument("--business-date")
    parser.add_argument("--result-path")
    args = parser.parse_args(argv)
    from shopvn.pipelines.inventory import run

    run(
        args.snapshot_id,
        mode=args.mode,
        business_date=args.business_date,
        result_path=args.result_path,
    )


if __name__ == "__main__":
    main()
