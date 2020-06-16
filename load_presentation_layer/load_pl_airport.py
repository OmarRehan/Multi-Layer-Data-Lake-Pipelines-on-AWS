from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import col
from sql_queries.sql_constants import dict_dbs_locations, dict_dbs_names


def load_pl_airport(spark, pl_loc,pl_name, il_name):
    delta_pl_airport = DeltaTable.forPath(spark, pl_loc + '/AIRPORT')

    df_LZ_airport = spark.sql(f"""
        SELECT AIRPORT_CODE,SPLIT(L_AIRPORT_SEQ_ID.DESCRIPTION,':')[1] AIRPORT_NAME,CITY_ID
        FROM
        (
            SELECT DISTINCT ORIGIN_AIRPORT_SEQ_ID AIRPORT_CODE,SPLIT(UPPER(ORIGIN_CITY_NAME),',')[0] CITY_NAME
            FROM {il_name}.FLIGHTS
            UNION
            SELECT DISTINCT DEST_AIRPORT_SEQ_ID,SPLIT(UPPER(DEST_CITY_NAME),',')[0] CITY_NAME
            FROM {il_name}.FLIGHTS
        ) SRC
        LEFT JOIN {il_name}.L_AIRPORT_SEQ_ID --To get Airport Name
        ON SRC.AIRPORT_CODE = L_AIRPORT_SEQ_ID.CODE
        LEFT OUTER JOIN {pl_name}.CITY
        ON SRC.CITY_NAME = CITY.CITY_NAME
        LEFT ANTI JOIN {pl_name}.AIRPORT -- To filter only delta 
        ON SRC.AIRPORT_CODE = AIRPORT.AIRPORT_CODE
    """)

    delta_pl_airport.alias("oldData") \
        .merge(df_LZ_airport.alias("newData"), "oldData.AIRPORT_CODE = newData.AIRPORT_CODE") \
        .whenMatchedUpdate(set={"AIRPORT_NAME": col("newData.AIRPORT_NAME"),"CITY_ID": col("newData.CITY_ID")}) \
        .whenNotMatchedInsert(values={
            "AIRPORT_CODE": col("newData.AIRPORT_CODE"),
            "AIRPORT_NAME": col("newData.AIRPORT_NAME"),
            "CITY_ID": col("newData.CITY_ID")
        }) \
        .execute()


if __name__ == '__main__':
    spark = initialize_spark_session('load_pl_airport')
    from delta.tables import *

    presentation_layer_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')
    presentation_layer_name = dict_dbs_names.get('PRESENTATION_LAYER_NAME')
    integration_layer_name = dict_dbs_names.get('INTEGRATION_LAYER_NAME')

    load_pl_airport(spark, presentation_layer_loc,presentation_layer_name, integration_layer_name)

