from pyspark.sql import SparkSession
import logging
import os


def initialize_spark_session(app_name):
    try:
        delta_jar_path = os.environ['DELTA_JAR_PATH']
    except Exception as e:
        logging.error(f"Failed to get delta jar Path,{e}")

    # Initializing a Spark session
    try:
        logging.info('initializing a spark session.')

        spark = SparkSession.builder \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()
        # Adding Delta Jar File Path to the context
        spark.sparkContext.addPyFile(delta_jar_path)

        return spark

    except Exception as e:
        logging.error(f"Failed to initialize a spark session,{e}")


if __name__ == '__main__':
    initialize_spark_session('initialize_spark_session_ut')
