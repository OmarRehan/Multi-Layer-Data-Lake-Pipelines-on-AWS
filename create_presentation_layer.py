import logging
from sql_queries.presentation_layer_ddl import ddl_create_presentation_layer_db, dict_pl_non_partitioned_tables, \
    schema_calendar,schema_flights
from constants import dict_dbs_locations, dict_dbs_names
from helper_functions.initialize_spark_session import initialize_spark_session
import os
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s ")

# TODO : Partition Flights Table on Year Month basis

if __name__ == '__main__':
    spark = initialize_spark_session('create_presentation_layer')

    from delta.tables import *

    # Creating the presentation_layer database in spark sql
    try:

        db_name = dict_dbs_names.get('PRESENTATION_LAYER_NAME')
        db_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')

        spark.sql(ddl_create_presentation_layer_db.format(presentation_layer_db_name=db_name,
                                                          presentation_layer_db_loc=db_loc))

        spark.sql(f'USE {db_name}')

        logging.info(f'{db_name} has been created.')

    except Exception as e:
        logging.error(f'Failed to create the {db_name} db in spark sql,{e}')

    # getting a list of tables names already in the db to check if they already exist
    try:
        df_exist_tables = spark.sql('SHOW TABLES')

    except Exception as e:
        logging.error(f'Failed to retrieve tables names,{e}')

    # creating presentation_layer non partitioned tables
    try:
        # Looping over the spark schemas to create them in HDFS
        for table_name, table_schema in dict_pl_non_partitioned_tables.items():
            # checking if the table already exists
            if df_exist_tables.filter(col("tableName").isin(table_name.lower())).select("tableName").count() == 0:
                df = spark.createDataFrame(spark.sparkContext.emptyRDD(), table_schema)

                table_loc = os.path.join(db_loc, table_name)
                # Saving the Dataframe into the corresponding path on HDFS
                df.write.format("delta").save(table_loc)

                # Creating the table in Spark SQL schema
                spark.sql(f"""CREATE TABLE {db_name}.{table_name} USING DELTA LOCATION '{table_loc}'""")

                logging.info(f'{table_name} has been created in {db_name}')

    except Exception as e:
        logging.error(f"Failed to create {table_name},{e}")

    # Creating Calendar Table & saving using partitions
    try:
        table_name_calendar = 'CALENDAR'

        # Checking if the table already exists
        if df_exist_tables.filter(col("tableName").isin(table_name_calendar.lower())).select("tableName").count() == 0:
            table_loc_calendar = os.path.join(db_loc, table_name_calendar)

            calendar_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_calendar)

            calendar_df.write.partitionBy(['YEAR', 'MONTH']).format("delta").save(table_loc_calendar)

            spark.sql(f"""CREATE TABLE {db_name}.{table_name_calendar} USING DELTA LOCATION '{table_loc_calendar}'""")

            logging.info(f'{table_name_calendar} has been created in {db_name}')

    except Exception as e:
        logging.error(f"Failed to create {table_name},{e}")

    # Creating Flights fact table, this is separated because this should be partitioned by date
    try:

        table_name_flights = 'FLIGHTS'

        # Checking if the table already exists
        if df_exist_tables.filter(col("tableName").isin(table_name_flights.lower())).select("tableName").count() == 0:
            flights_loc = os.path.join(db_loc, table_name_flights)

            # An empty df with a table schema to save it as delta, as current delta supports creating tables using dataframe syntax only
            flights_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_flights)

            # Saving the Dataframe into the corresponding path on HDFS
            flights_df.write.partitionBy(['FLIGHT_YEARMON']).format("delta").save(flights_loc)

            # Creating the table in Spark SQL schema
            spark.sql(f"""CREATE TABLE {db_name}.FLIGHTS USING DELTA LOCATION '{flights_loc}'""")

            logging.info(f'FLIGHTS has been created in {db_name}')

    except Exception as e:
        logging.error(f"Failed to create table,{e}")
