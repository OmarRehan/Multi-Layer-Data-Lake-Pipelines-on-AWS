import logging
import sys
import os
from helper_functions.initialize_spark_session import initialize_spark_session
from constants import edge_node_path, dict_dbs_locations


def load_lz_flights(spark, yearmonth, edge_node_loc, lz_loc):
    flights_table_name = 'FLIGHTS'

    try:

        df_flights = spark.read.format('parquet').option('compression', 'gzip').load(
            os.path.join(edge_node_loc, flights_table_name, yearmonth+'.gz'))

        df_flights.write.format('parquet') \
            .mode("overwrite") \
            .option("compression", "gzip") \
            .save(os.path.join(lz_loc, flights_table_name))

        logging.info(f'{flights_table_name} has been loaded in the landing zone.')

    except Exception as e:
        logging.error(f"Failed to load Flights in the landing zone,{e}")
        spark.stop()
        raise Exception(f'Failed to create Flights spark dataframe, {e}')


if __name__ == '__main__':

    yearmonth = None
    # finding and parsing the yearmonth argument for flights table partition
    for arg in sys.argv:
        if 'yearmonth' in arg:
            yearmonth = arg.split('=')[1]

    if yearmonth is not None:

        spark = initialize_spark_session('load_lz_city_demographics')

        landing_zone_loc = dict_dbs_locations.get('LANDING_ZONE_LOC')

        load_lz_flights(spark, yearmonth, edge_node_path, landing_zone_loc)

    else:
        raise Exception('year month yyyymm argument is required to start loading, ex: yearmonth=202006')
