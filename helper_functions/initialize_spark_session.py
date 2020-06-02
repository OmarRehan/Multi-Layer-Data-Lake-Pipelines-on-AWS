from pyspark.sql import SparkSession
import logging


def initialize_spark_session():
    # Initializing a Spark session
    try:
        logging.info('initializing a spark session.')

        return SparkSession.builder \
            .appName("create_landing_zone") \
            .enableHiveSupport() \
            .getOrCreate()

    except Exception as e:
        logging.error(f"Failed to initialize a spark session,{e}")
