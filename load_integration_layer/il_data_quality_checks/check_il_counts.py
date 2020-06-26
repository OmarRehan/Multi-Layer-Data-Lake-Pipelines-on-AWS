import logging
from helper_functions.initialize_spark_session import initialize_spark_session
from constants import dict_dbs_names


def check_il_counts(spark, integration_layer_name):
    pd_df_counts = spark.sql(f"""
        SELECT 'CITY_DEMOGRAPHICS' ,COUNT(*) COUNT FROM {integration_layer_name}.CITY_DEMOGRAPHICS
        UNION
        SELECT 'FLIGHTS' ,COUNT(*) COUNT FROM {integration_layer_name}.FLIGHTS
        UNION
        SELECT 'L_AIRLINE_ID',COUNT(*) COUNT FROM {integration_layer_name}.L_AIRLINE_ID
        UNION
        SELECT 'L_AIRPORT',COUNT(*) COUNT FROM {integration_layer_name}.L_AIRPORT
        UNION 
        SELECT 'L_AIRPORT_ID',COUNT(*) COUNT FROM {integration_layer_name}.L_AIRPORT_ID
        UNION
        SELECT 'L_AIRPORT_SEQ_ID',COUNT(*) COUNT FROM {integration_layer_name}.L_AIRPORT_SEQ_ID
        UNION
        SELECT 'L_CANCELLATION',COUNT(*) COUNT FROM {integration_layer_name}.L_CANCELLATION
        UNION
        SELECT 'L_CARRIER_HISTORY',COUNT(*) COUNT FROM {integration_layer_name}.L_CARRIER_HISTORY
        UNION
        SELECT 'L_CITY_MARKET_ID',COUNT(*) COUNT FROM {integration_layer_name}.L_CITY_MARKET_ID
        UNION
        SELECT 'L_DEPARRBLK',COUNT(*) COUNT FROM {integration_layer_name}.L_DEPARRBLK
        UNION
        SELECT 'L_DISTANCE_GROUP_250',COUNT(*) COUNT FROM {integration_layer_name}.L_DISTANCE_GROUP_250
        UNION
        SELECT 'L_DIVERSIONS',COUNT(*) COUNT FROM {integration_layer_name}.L_DIVERSIONS
        UNION
        SELECT 'L_MONTHS',COUNT(*) COUNT FROM {integration_layer_name}.L_MONTHS
        UNION
        SELECT 'L_ONTIME_DELAY_GROUPS',COUNT(*) COUNT FROM {integration_layer_name}.L_ONTIME_DELAY_GROUPS
        UNION
        SELECT 'L_QUARTERS',COUNT(*) COUNT FROM {integration_layer_name}.L_QUARTERS
        UNION
        SELECT 'L_STATE_ABR_AVIATION',COUNT(*) COUNT FROM {integration_layer_name}.L_STATE_ABR_AVIATION
        UNION
        SELECT 'L_STATE_FIPS',COUNT(*) COUNT FROM {integration_layer_name}.L_STATE_FIPS
        UNION
        SELECT 'L_UNIQUE_CARRIERS',COUNT(*) COUNT FROM {integration_layer_name}.L_UNIQUE_CARRIERS
        UNION
        SELECT 'L_WEEKDAYS',COUNT(*) COUNT FROM {integration_layer_name}.L_WEEKDAYS
        UNION
        SELECT 'L_WORLD_AREA_CODES',COUNT(*) COUNT FROM {integration_layer_name}.L_WORLD_AREA_CODES
        UNION
        SELECT 'L_YESNO_RESP',COUNT(*) COUNT FROM {integration_layer_name}.L_YESNO_RESP
    """).toPandas()

    df_empty_tables = pd_df_counts[pd_df_counts.COUNT == 0]
    empty_tables_count = len(df_empty_tables)

    if empty_tables_count > 0:
        raise Exception(f'{empty_tables_count} Tables are empty in Integration Layer\n{df_empty_tables.to_string()}')
    else:
        logging.info('\n' + pd_df_counts.to_string())


if __name__ == '__main__':
    spark = initialize_spark_session('check_il_counts')
    from delta.tables import *

    integration_layer_name = dict_dbs_names.get('INTEGRATION_LAYER_NAME')
    check_il_counts(spark, integration_layer_name)
