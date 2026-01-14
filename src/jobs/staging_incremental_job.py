"""
Cron: incremental staging
"""

from src.load_staging.staging_incremental import load_staging_incremental


def main():
    load_staging_incremental()


if __name__ == "__main__":
    main()
