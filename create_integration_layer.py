import logging
from sql_queries.integration_layer_ddl import ddl_create_integration_layer_db, dict_integration_layer_tables
from sql_queries.sql_constants import dict_dbs_locations, dict_dbs_names
from helper_functions.initialize_spark_session import initialize_spark_session
from helper_functions.read_configs_file import read_configs_file
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s ")

# TODO : Partition Flights Table on Year Month basis

if __name__ == '__main__':
    spark = initialize_spark_session('create_integration_layer')

    from delta.tables import *

    config = read_configs_file()

    # Creating the integration_layer database in spark sql
    try:

        db_name = dict_dbs_names.get('INTEGRATION_LAYER_NAME')
        db_loc = dict_dbs_locations.get('INTEGRATION_LAYER_LOC')

        spark.sql(ddl_create_integration_layer_db.format(integration_layer_db_name=db_name,integration_layer_db_loc=db_loc))

        spark.sql(f'USE {db_name}')

        logging.info(f'{db_name} has been created.')

    except Exception as e:
        logging.error(f'Failed to create the {db_name} db in spark sql,{e}')

    # creating integration_layer tables
    try:
        # Looping over the spark schemas to create them in HDFS
        for table_name, table_schema in dict_integration_layer_tables.items():
            table_loc = os.path.join(db_loc, table_name)

            # An empty df with a table schema to save it as delta, as current delta supports creating tables using dataframe syntax only
            df = spark.createDataFrame(spark.sparkContext.emptyRDD(), table_schema)

            # Saving the Dataframe into the corresponding path on HDFS
            df.write.format("delta").save(table_loc)

            # Creating the table in Spark SQL schema
            spark.sql(f"""CREATE TABLE {db_name}.{table_name} USING DELTA LOCATION '{table_loc}'""")

            logging.info(f'{table_name} has been created in {db_name}')

    except Exception as e:
        logging.error(f"Failed to create table,{e}")
