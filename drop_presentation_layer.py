import logging
from helper_functions.initialize_spark_session import initialize_spark_session
from sql_queries.integration_layer_ddl import ddl_drop_integration_layer_db
from constants import dict_dbs_names

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

if __name__ == '__main__':

    spark = initialize_spark_session('drop_presentation_layer')

    try:
        db_name = dict_dbs_names.get('PRESENTATION_LAYER_NAME')

        spark.sql(ddl_drop_integration_layer_db.format(integration_layer_db_name=db_name))
        logging.info(f'The {db_name} Db has been Dropped')

    except Exception as e:
        logging.error(f"Failed to drop the {db_name},{e}")
