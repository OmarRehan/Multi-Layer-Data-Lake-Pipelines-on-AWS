import logging
from sql_queries.landing_zone_ddl import ddl_create_land_zone_db,dict_landing_zone_ddls
from constants import dict_dbs_locations, dict_dbs_names
from helper_functions.initialize_spark_session import initialize_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s ")

if __name__ == '__main__':

    spark = initialize_spark_session('create_landing_zone')

    # Creating the landing_zone database in spark sql
    try:

        db_name = dict_dbs_names.get('LANDING_ZONE_NAME')
        db_loc = dict_dbs_locations.get('LANDING_ZONE_LOC')

        spark.sql(ddl_create_land_zone_db.format(landing_zone_db_name=db_name,landing_zone_db_loc=db_loc))

        logging.info(f'{db_name} has been created.')

    except Exception as e:
        logging.error(f'Failed to create the {db_name} db in spark sql,{e}')

    # creating landing zone tables
    try:

        for table_name,table_ddl in dict_landing_zone_ddls.items():
            spark.sql(table_ddl.format(landing_zone_db_name=db_name,landing_zone_db_loc=db_loc))

            logging.info(f'{table_name} has been created in {db_name}')

    except Exception as e:
        logging.error(f"Failed to create table,{e}")
