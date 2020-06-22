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
