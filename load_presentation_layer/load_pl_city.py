from helper_functions.initialize_spark_session import initialize_spark_session
from sql_queries.sql_constants import dict_dbs_locations, dict_dbs_names
from pyspark.sql.functions import monotonically_increasing_id


def load_pl_city(spark, pl_loc,pl_name, il_name):

    df_LZ_city = spark.sql(f"""
        SELECT TRIM(CITY_NAME) CITY_NAME,STATE_ABR
        FROM
        (
            SELECT DISTINCT 
            SPLIT(UPPER(ORIGIN_CITY_NAME),',')[0] CITY_NAME,
            ORIGIN_STATE_ABR STATE_ABR
            FROM {il_name}.FLIGHTS
            UNION
            SELECT DISTINCT 
            SPLIT(UPPER(DEST_CITY_NAME),',')[0] CITY_NAME,
            DEST_STATE_ABR STATE_ABR
            FROM {il_name}.FLIGHTS
            
            --CITY DEMOGRAPHICS DELTA
            UNION
            SELECT 
            TRIM(UPPER(CITY_DEMOGRAPHICS.CITY)) CITY_NAME,STATE_CODE STATE_ABR
            FROM {il_name}.CITY_DEMOGRAPHICS
            LEFT ANTI JOIN {pl_name}.CITY
            ON TRIM(UPPER(CITY_DEMOGRAPHICS.CITY)) = CITY.CITY_NAME
            AND CITY_DEMOGRAPHICS.STATE_CODE = CITY.STATE_ABR

        ) SRC
        LEFT ANTI JOIN {pl_name}.CITY
        ON SRC.CITY_NAME = CITY.CITY_NAME
        AND SRC.STATE_ABR = CITY.STATE_ABR
    """)

    df_LZ_city = df_LZ_city.withColumn("CITY_ID",monotonically_increasing_id())

    df_LZ_city.write.format("delta").mode("append").save(pl_loc + '/CITY')


if __name__ == '__main__':
    spark = initialize_spark_session('load_pl_city')
    from delta.tables import *

    presentation_layer_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')
    presentation_layer_name = dict_dbs_names.get('PRESENTATION_LAYER_NAME')
    integration_layer_name = dict_dbs_names.get('INTEGRATION_LAYER_NAME')

    load_pl_city(spark, presentation_layer_loc,presentation_layer_name, integration_layer_name)

