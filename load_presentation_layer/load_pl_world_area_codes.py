from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import col
from sql_queries.sql_constants import dict_dbs_locations, dict_dbs_names
from sql_queries.sql_constants import missing_val_replace_alphanumeric,missing_val_replace_numeric


def load_pl_world_area_codes(spark, pl_loc, il_name):
    delta_pl_world_area_codes = DeltaTable.forPath(spark, pl_loc + '/WORLD_AREA_CODES')

    df_LZ_l_world_area_codes = spark.sql(f"""
        SELECT 
        CODE
        ,DESCRIPTION
        FROM {il_name}.L_WORLD_AREA_CODES
    """)

    delta_pl_world_area_codes.alias("oldData") \
        .merge(df_LZ_l_world_area_codes.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"NAME": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "NAME": col("newData.DESCRIPTION")}) \
        .execute()


if __name__ == '__main__':
    spark = initialize_spark_session('load_pl_world_area_codes')
    from delta.tables import *

    presentation_layer_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')
    integration_layer_name = dict_dbs_names.get('INTEGRATION_LAYER_NAME')

    load_pl_world_area_codes(spark, presentation_layer_loc, integration_layer_name)

