"""Spark-submit CLI for publication of an approved candidate snapshot."""

import argparse


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-key", required=True)
    args = parser.parse_args(argv)
    from shopvn.marts.publication import publish

    publish(args.run_key)


if __name__ == "__main__":
    main()
