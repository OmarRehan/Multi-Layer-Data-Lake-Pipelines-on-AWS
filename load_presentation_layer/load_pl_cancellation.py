import logging

from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import col
from constants import dict_dbs_locations, dict_dbs_names


def load_pl_cancellation(spark, pl_loc, il_name):
    try:
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

        logging.info('CANCELLATION has been loaded in the Presentation layer')

    except Exception as e:
        logging.error('Failed to load CANCELLATION in the Presentation Layer')
        spark.stop()
        raise Exception(f'Failed to load CANCELLATION in the Presentation Layer,{e}')


if __name__ == '__main__':
    spark = initialize_spark_session('load_pl_cancellation')
    from delta.tables import *

    try:

        presentation_layer_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')
        integration_layer_name = dict_dbs_names.get('INTEGRATION_LAYER_NAME')

    except Exception as e:
        logging.error('Failed to retrieve Environment variables')
        spark.stop()
        raise Exception(f'Failed to load CANCELLATION in the Presentation Layer,{e}')

    load_pl_cancellation(spark, presentation_layer_loc, integration_layer_name)
