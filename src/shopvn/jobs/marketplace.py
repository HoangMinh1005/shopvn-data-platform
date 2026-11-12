"""Import-safe Spark entry point; ingestion/Paramiko never imported by Spark."""

import argparse


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("prepare", "publish"))
    parser.add_argument("--capture-id")
    parser.add_argument("--run-key", required=True)
    args = parser.parse_args(argv)
    from shopvn.pipelines.marketplace import prepare, publish

    if args.action == "prepare":
        prepare(args.capture_id, args.run_key)
    else:
        publish(args.run_key)


if __name__ == "__main__":
    main()
