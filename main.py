import argparse
import logging
import time

import kaggle_uploader
import source_loader

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description="Data Loader Script")
parser.add_argument('--csv', action='store_true', help="Save data as CSV")
parser.add_argument('--parquet', action='store_true', help="Save data as Parquet")
args = parser.parse_args()


while True:
    
    logging.info("Starting data load and upload process")
    
    source_loader.execute(csv=args.csv, parquet=args.parquet)
    kaggle_uploader.execute()

    logging.info("Process completed, sleeping for 6 hours")
    time.sleep(60*60*6)