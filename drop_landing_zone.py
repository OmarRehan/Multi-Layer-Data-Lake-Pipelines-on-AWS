import logging
from helper_functions.initialize_spark_session import initialize_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

if __name__ == '__main__':
    # TODO : Update this as in drop_integration_layer.py
    spark = initialize_spark_session('drop_landing_zone')

    try:

        spark.sql("""DROP DATABASE LANDING_ZONE CASCADE""")
        logging.info('The Landing Zone Db has been Dropped')

    except Exception as e:
        logging.error(f"Failed to drop the Landing Zone,{e}")
