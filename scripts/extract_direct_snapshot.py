"""Local CLI wrapper for the read-only PostgreSQL source adapter."""

from shopvn.ingestion.postgres import extract

if __name__ == "__main__":
    extract()
