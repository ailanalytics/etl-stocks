"""
Cron: incremental EOD → S3
"""

from src.load_raw.s3.write_incremental_s3 import get_incremental_data

def main():
    get_incremental_data()

if __name__ == "__main__":
    main()
