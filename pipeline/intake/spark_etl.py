import argparse
from pyspark.sql import SparkSession

from intake.etl import run_etl


def get_args():
    parser = argparse.ArgumentParser(description="Spark ETL CLI")
    parser.add_argument('--ftp_path', help="path to file on ftp site (https://gml.noaa.gov/aftp/)", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    run_etl(args.ftp_path)