import logging
from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.types import StructField, StructType, StringType
from constants import dict_dbs_locations, edge_node_path
from sql_queries.landing_zone_ddl import list_landing_zone_standard_lookups
from helper_functions.zip_csv_to_gzip_parquet import zip_csv_to_gzip_parquet
from helper_functions.loop_files import loop_files
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s ")

if __name__ == '__main__':

    # Initializing a Spark session
    spark = initialize_spark_session('load_landing_zone')

    landing_zone_location = dict_dbs_locations.get('LANDING_ZONE_LOC')

    # Loading the standard lookups with the same schema in landing_zone
    try:

        # Standard schema for the standard lookups
        schema_lookups_schema = StructType(
            [
                StructField("Code", StringType(), True),
                StructField("Description", StringType(), True)
            ]
        )

        # Loops over all the standard lookups to load them
        for table_name in list_landing_zone_standard_lookups:
            df_file = spark \
                .read \
                .schema(schema_lookups_schema) \
                .option("header", "true") \
                .csv(os.path.join(edge_node_path, table_name, '*.csv'))

            df_file.write.format("csv") \
                .mode("overwrite") \
                .option("sep", ",") \
                .option('header', 'true') \
                .save(os.path.join(landing_zone_location, table_name))

            logging.info(f'{table_name} has been loaded in the landing zone.')

    except Exception as e:
        logging.error(f"Failed to load {table_name} in the landing zone,{e}")

    # TODO : source file should be in .zip format with yyyymm format
    try:

        flights_table_name = 'FLIGHTS'

        # Looping over the zip files in the Edge Node directory
        list_zip_files = loop_files(os.path.join(edge_node_path, flights_table_name), '*.zip')

        # get total number of files found
        num_files = len(list_zip_files)
        logging.info(' {} files found in {}'.format(num_files, os.path.join(edge_node_path, flights_table_name)))

        # Transforming the available csv zip files into parquet gzip
        for file in list_zip_files:
            zip_csv_to_gzip_parquet(file)
            logging.info(f'{file} transformed into parquet gzip')

        df_flights = spark.read.format('parquet').option('compression', 'gzip').load(
            os.path.join(edge_node_path, flights_table_name, '*.gz'))

        df_flights.write.format('parquet') \
            .mode("overwrite") \
            .option("compression", "gzip") \
            .save(os.path.join(landing_zone_location, flights_table_name))

        logging.info(f'{flights_table_name} has been loaded in the landing zone.')

    except Exception as e:
        logging.error(f"Failed to load Flights in the landing zone,{e}")
