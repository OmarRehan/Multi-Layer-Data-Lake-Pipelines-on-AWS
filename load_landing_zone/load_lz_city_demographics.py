import logging
import pandas as pd
import os
from helper_functions.initialize_spark_session import initialize_spark_session
from constants import edge_node_path,dict_dbs_locations


def load_lz_city_demographics(spark,edge_node_loc,lz_loc):
    table_name = 'CITY_DEMOGRAPHICS'

    try:
        # Reading the source JSON file
        df_city_demo = pd.read_json(os.path.join(edge_node_loc, 'CITY_DEMOGRAPHICS/us-cities-demographics.json'))

        # Extracting the files from 'fields' colmn
        df_city_demo = pd.DataFrame.from_dict(df_city_demo['fields'].to_list(), orient='columns')

    except Exception as e:
        logging.error(f'failed to open the Directory of source data, {e}')

    try:

        spark.createDataFrame(df_city_demo).write.format("csv") \
            .mode("overwrite") \
            .option('header', 'true') \
            .save(os.path.join(lz_loc, table_name))

    except Exception as e:
        logging.error(f'Failed to create spark dataframe, {e}')


if __name__ == '__main__':
    print('Hi')
    spark = initialize_spark_session('load_lz_city_demographics')

    landing_zone_loc = dict_dbs_locations.get('LANDING_ZONE_LOC')

    load_lz_city_demographics(spark,edge_node_path,landing_zone_loc)
