from pyspark.sql import SparkSession
import logging


def initialize_spark_session(app_name):
    # Initializing a Spark session
    try:
        logging.info('initializing a spark session.')

        spark = SparkSession.builder \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()
        # TODO : Add delta jar path to an environment variable
        spark.sparkContext.addPyFile(
            '/home/admin_123/Binaries/spark-2.4.4-bin-hadoop2.7/jars/delta-core_2.11-0.6.1.jar')

        return spark

    except Exception as e:
        logging.error(f"Failed to initialize a spark session,{e}")
