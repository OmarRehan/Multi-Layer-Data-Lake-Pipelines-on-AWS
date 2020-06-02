from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")


if __name__ == '__main__':

    try:
        logging.info('initializing a spark session.')

        spark = SparkSession.builder \
            .appName("drop_landing_zone") \
            .enableHiveSupport()\
            .getOrCreate()

    except Exception as e:
        logging.error(f"Failed to initialize a spark session,{e}")

    try:

        spark.sql("""DROP DATABASE LANDING_ZONE""")
        logging.info('Landing Zone Db has been Dropped')

    except Exception as e:
        logging.error(f"Failed to initialize a spark session,{e}")