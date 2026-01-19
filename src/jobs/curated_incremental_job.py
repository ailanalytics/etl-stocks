"""
Cron: incremental curated
"""

from src.load_curated.curated_incremental import load_curated_incremental


def main():
    load_curated_incremental()


if __name__ == "__main__":
    main()
