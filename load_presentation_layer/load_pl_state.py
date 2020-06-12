from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import col
from sql_queries.sql_constants import dict_dbs_locations, dict_dbs_names


def load_pl_state(spark, pl_loc, il_name):
    delta_pl_state = DeltaTable.forPath(spark, pl_loc + '/STATE')

    df_LZ_state = spark.sql(f"""
        SELECT DISTINCT
        ORIGIN_STATE_ABR STATE_ABR,
        ORIGIN_STATE_FIPS STATE_FIPS,
        ORIGIN_STATE_NM STATE_NAME,
        ORIGIN_WAC WAC_CODE
        FROM
        (
            SELECT DISTINCT ORIGIN_STATE_ABR,ORIGIN_STATE_FIPS,ORIGIN_STATE_NM,ORIGIN_WAC
            FROM {il_name}.FLIGHTS
            UNION
            SELECT DISTINCT DEST_STATE_ABR,DEST_STATE_FIPS,DEST_STATE_NM,DEST_WAC
            FROM {il_name}.FLIGHTS
        ) 
    """)

    delta_pl_state.alias("oldData") \
        .merge(df_LZ_state.alias("newData"), "oldData.STATE_ABR = newData.STATE_ABR") \
        .whenMatchedUpdate(set={
            "STATE_FIPS": col("newData.STATE_FIPS"),
            "STATE_NAME": col("newData.STATE_NAME"),
            "WAC_CODE": col("newData.WAC_CODE")
        }) \
        .whenNotMatchedInsert(values={
            "STATE_ABR": col("newData.STATE_ABR"),
            "STATE_FIPS": col("newData.STATE_FIPS"),
            "STATE_NAME": col("newData.STATE_NAME"),
            "WAC_CODE": col("newData.WAC_CODE")
        }) \
        .execute()


if __name__ == '__main__':
    spark = initialize_spark_session('load_pl_state')
    from delta.tables import *

    presentation_layer_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')
    integration_layer_name = dict_dbs_names.get('INTEGRATION_LAYER_NAME')

    load_pl_state(spark, presentation_layer_loc, integration_layer_name)
