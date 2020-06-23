import logging
from helper_functions.initialize_spark_session import initialize_spark_session
from constants import dict_dbs_names


def check_pl_counts(spark, presentation_layer_name):

    pd_df_counts = spark.sql(f"""
        SELECT 'CITY_DEMOGRAPHICS' ,COUNT(*) COUNT FROM {presentation_layer_name}.CALENDAR
        UNION
        SELECT 'FLIGHTS' ,COUNT(*) COUNT FROM {presentation_layer_name}.FLIGHTS
        UNION
        SELECT 'L_AIRLINE_ID',COUNT(*) COUNT FROM {presentation_layer_name}.CANCELLATION
        UNION
        SELECT 'L_AIRPORT',COUNT(*) COUNT FROM {presentation_layer_name}.WORLD_AREA_CODES
        UNION 
        SELECT 'L_AIRPORT_ID',COUNT(*) COUNT FROM {presentation_layer_name}.STATE
        UNION
        SELECT 'L_AIRPORT_SEQ_ID',COUNT(*) COUNT FROM {presentation_layer_name}.CITY
        UNION
        SELECT 'L_CANCELLATION',COUNT(*) COUNT FROM {presentation_layer_name}.AIRPORT
        UNION
        SELECT 'L_CARRIER_HISTORY',COUNT(*) COUNT FROM {presentation_layer_name}.AIRLINE
        UNION
        SELECT 'L_CITY_MARKET_ID',COUNT(*) COUNT FROM {presentation_layer_name}.CITY_DEMOGRAPHICS
    """).toPandas()

    df_empty_tables = pd_df_counts[pd_df_counts.COUNT == 0]
    empty_tables_count = len(df_empty_tables)

    if empty_tables_count > 0:
        raise Exception(f'{empty_tables_count} Tables are empty in {presentation_layer_name}\n{df_empty_tables.to_string()}')
    else:
        logging.info('\n'+pd_df_counts.to_string())


if __name__ == '__main__':
    spark = initialize_spark_session('check_pl_counts')
    from delta.tables import *

    try:
        presentation_layer_name = dict_dbs_names.get('PRESENTATION_LAYER')

    except Exception as e:
        logging.error('Failed to retrieve Environment variables')

    check_pl_counts(spark, presentation_layer_name)
