from pyspark.sql import SparkSession
import logging


def initialize_spark_session(app_name):
    # Initializing a Spark session
    try:
        logging.info('initializing a spark session.')

        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1") \
            .enableHiveSupport() \
            .getOrCreate()

    except Exception as e:
        logging.error(f"Failed to initialize a spark session,{e}")
