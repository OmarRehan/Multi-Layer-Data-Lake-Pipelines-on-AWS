from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import col
from constants import dict_dbs_locations, dict_dbs_names


def load_l_airline_id(spark, integration_layer_loc, landing_zone_name):
    delta_l_airline_id = DeltaTable.forPath(spark, integration_layer_loc + '/L_AIRLINE_ID')

    df_LZ_l_airline_id = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_AIRLINE_ID
    """)

    delta_l_airline_id.alias("oldData") \
        .merge(df_LZ_l_airline_id.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


if __name__ == '__main__':
    spark = initialize_spark_session('load_il_airline_id')
    from delta.tables import *

    integration_layer_loc = dict_dbs_locations.get('INTEGRATION_LAYER_LOC')
    landing_zone_name = dict_dbs_names.get('LANDING_ZONE_NAME')

    load_l_airline_id(spark, integration_layer_loc, landing_zone_name)
