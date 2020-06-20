import logging

from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import col
from constants import dict_dbs_locations, dict_dbs_names


def load_pl_airline(spark, pl_loc, il_name):
    try:
        delta_pl_airline = DeltaTable.forPath(spark, pl_loc + '/AIRLINE')

        df_LZ_l_airline_id = spark.sql(f"""
            SELECT 
            CODE AIRLINE_ID,
            SPLIT(DESCRIPTION,'.:')[0] AIRLINE_NAME,
            SPLIT(DESCRIPTION,'.:')[1] AS AIRLINE_CODE
            FROM {il_name}.L_AIRLINE_ID
        """)

        delta_pl_airline.alias("oldData") \
            .merge(df_LZ_l_airline_id.alias("newData"), "oldData.AIRLINE_ID = newData.AIRLINE_ID") \
            .whenMatchedUpdate(set={
                "AIRLINE_NAME": col("newData.AIRLINE_NAME"),
                "AIRLINE_CODE": col("newData.AIRLINE_CODE")
            }) \
            .whenNotMatchedInsert(values={
                "AIRLINE_ID": col("newData.AIRLINE_ID"),
                "AIRLINE_NAME": col("newData.AIRLINE_NAME"),
                "AIRLINE_CODE": col("newData.AIRLINE_CODE")
            }) \
            .execute()

        logging.info('AIRLINE has been loaded in the Presentation layer')

    except Exception as e:
        logging.error('Failed to load AIRLINE in the Presentation Layer')
        spark.stop()
        raise Exception(f'Failed to load Airline in the Presentation Layer,{e}')


if __name__ == '__main__':
    spark = initialize_spark_session('load_pl_airline')
    from delta.tables import *

    try:

        presentation_layer_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')
        integration_layer_name = dict_dbs_names.get('INTEGRATION_LAYER_NAME')

    except Exception as e:
        logging.error('Failed to retrieve Environment variables')
        spark.stop()
        raise Exception(f'Failed to load Airline in the Presentation Layer,{e}')

    load_pl_airline(spark, presentation_layer_loc, integration_layer_name)

