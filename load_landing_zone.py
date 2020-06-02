import logging
from helper_functions.initialize_spark_session import initialize_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s ")

if __name__ == '__main__':

    # Initializing a Spark session
    spark = initialize_spark_session()

    # Loading lookups in landing_zone
    try:

        df_l_airline_id = spark \
            .read \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .csv("/home/admin_123/Desktop/Main/DataSets/FlightsData/L_AIRLINE_ID/*.csv")

        df_l_airline_id.write.format("csv") \
            .mode("overwrite") \
            .option("sep", ",") \
            .option('header', 'true') \
            .save("hdfs://localhost:9000/FLIGHTS_DL/LANDING_ZONE/L_AIRLINE_ID")

    except Exception as e:
        logging.error(f"Failed to create the landing_zone db in spark sql,{e}")
