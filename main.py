import argparse
import logging
import time

import kaggle_uploader
import source_loader
import s3_uploader
from nekt_loader import nekt_to_mysql

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description="Data Loader Script")
parser.add_argument('--csv', action='store_true', help="Save data as CSV")
parser.add_argument('--parquet', action='store_true', help="Save data as Parquet")
args = parser.parse_args()


while True:
    
    logging.info("Starting data load and upload process")
    
    logging.info("   Carregando dados do banco...")
    source_loader.execute(csv=args.csv, parquet=args.parquet)

    logging.info("   Enviando dados para o Kaggle...")
    kaggle_uploader.execute()

    logging.info("   Enviando dados para o S3...")
    s3_uploader.execute()
    
    logging.info("   Enviando dados para a feature store do MySQL...")

    logging.info("       Enviando tmw_user...")
    nekt_to_mysql(
        layer_name="Silver",
        t_nekt="fs_all",
        t_fs="tmw_user",
        schema="feature_store",
        renames={"idcliente": "id"}
    )

    logging.info("Process completed, sleeping for 4 hours")
    time.sleep(60*60*4)