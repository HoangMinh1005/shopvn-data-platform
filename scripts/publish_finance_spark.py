"""Backward-compatible spark-submit entry point; logic lives in the package."""

from shopvn.jobs.publish_finance import main

if __name__ == "__main__":
    main()
