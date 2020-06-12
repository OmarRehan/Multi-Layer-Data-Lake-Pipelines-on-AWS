from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import col
from sql_queries.sql_constants import dict_dbs_locations, dict_dbs_names
from sql_queries.sql_constants import missing_val_replace_alphanumeric,missing_val_replace_numeric


def load_pl_cancellation(spark, pl_loc, il_name):
    delta_pl_cancellation = DeltaTable.forPath(spark, pl_loc + '/CANCELLATION')

    df_LZ_l_cancellation = spark.sql(f"""
        SELECT 
        CODE
        ,DESCRIPTION
        FROM {il_name}.L_CANCELLATION
    """)

    delta_pl_cancellation.alias("oldData") \
        .merge(df_LZ_l_cancellation.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


if __name__ == '__main__':
    spark = initialize_spark_session('load_pl_cancellation')
    from delta.tables import *

    presentation_layer_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')
    integration_layer_name = dict_dbs_names.get('INTEGRATION_LAYER_NAME')

    load_pl_cancellation(spark, presentation_layer_loc, integration_layer_name)

