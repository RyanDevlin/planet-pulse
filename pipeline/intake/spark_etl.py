import argparse
from pyspark.sql import SparkSession

from intake.etl import run_etl


def get_args():
    parser = argparse.ArgumentParser(description="Spark ETL CLI")
    parser.add_argument('--source', help="source file type (should be name of module in pipeline/intake/sources/)", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    spark = SparkSession.builder.getOrCreate()
    run_etl(args.source, spark=spark)