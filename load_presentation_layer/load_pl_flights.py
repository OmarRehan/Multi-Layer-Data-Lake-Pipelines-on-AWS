import logging

from helper_functions.initialize_spark_session import initialize_spark_session
from constants import dict_dbs_locations, dict_dbs_names
import sys


def load_pl_flights(spark,pl_loc,il_name, yearmonth):

    try:
        # TODO : Add Where clause to the select query to load only the specified year month
        df_il_flights = spark.sql(f"""
                SELECT
                CONCAT_WS('::',FL_DATE,OP_CARRIER_AIRLINE_ID,OP_CARRIER_FL_NUM,ORIGIN_AIRPORT_ID,DEST_AIRPORT_ID) FLIGHT_ID
                ,CAST(REPLACE(FL_DATE,'-','') AS INTEGER) FLIGHT_DATE
                ,OP_CARRIER_AIRLINE_ID AIRLINE_ID
                ,TAIL_NUM
                ,OP_CARRIER_FL_NUM FLIGHT_NUM
                ,ORIGIN_AIRPORT_SEQ_ID ORIGIN_AIRPORT_ID
                ,DEST_AIRPORT_SEQ_ID DEST_AIRPORT_ID
                ,LPAD(CAST(DEP_TIME AS INTEGER),4,'0') DEPARTURE_TIME
                ,CASE WHEN DEP_DELAY < 0 THEN CAST(ABS(DEP_DELAY) AS INTEGER) ELSE 0 END EARLY_DEPARTURE_MINS
                ,CASE WHEN DEP_DELAY > 0 THEN CAST(ABS(DEP_DELAY) AS INTEGER) ELSE 0 END DELAY_DEPARTURE_MINS
                ,TAXI_OUT TAXI_OUT_MINS
                ,TAXI_IN TAXI_IN_MINS
                ,LPAD(CAST(WHEELS_OFF AS INTEGER),4,'0') WHEELS_OFF_TIME
                ,LPAD(CAST(WHEELS_ON AS INTEGER),4,'0') WHEELS_ON_TIME
                ,LPAD(CAST(ARR_TIME AS INTEGER),4,'0') ARR_TIME
                ,CASE WHEN ARR_DELAY < 0 THEN CAST(ABS(ARR_DELAY) AS INTEGER) ELSE 0 END EARLY_ARRIVAL_MINS
                ,CASE WHEN ARR_DELAY > 0 THEN CAST(ABS(ARR_DELAY) AS INTEGER) ELSE 0 END DELAY_ARRIVAL_MINS
                ,CANCELLATION_CODE
                ,ACTUAL_ELAPSED_TIME FLIGHT_ELAPSED_TIME
                ,AIR_TIME
                ,DISTANCE DISTANCE_MILES
                ,CAST(CARRIER_DELAY AS INTEGER) CARRIER_DELAY_MINS
                ,CAST(WEATHER_DELAY AS INTEGER) WEATHER_DELAY_MINS
                ,CAST(NAS_DELAY AS INTEGER) NAS_DELAY_MINS
                ,CAST(SECURITY_DELAY AS INTEGER) SECURITY_DELAY_MINS
                ,CAST(LATE_AIRCRAFT_DELAY AS INTEGER) LATE_AIRCRAFT_DELAY_MINS
                ,CAST(TOTAL_ADD_GTIME AS INTEGER) TOTAL_ADD_GTIME_MINS
                ,CAST(CONCAT(YEAR(FL_DATE),LPAD(MONTH(FL_DATE),2,'0')) AS INTEGER) FLIGHT_YEARMON
                FROM {il_name}.FLIGHTS
                WHERE YEAR = SUBSTR({yearmonth},1,4) AND MONTH = SUBSTR({yearmonth},5,6)
            """)

        df_il_flights.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere",
                    f"FLIGHT_YEARMON = {yearmonth}") \
            .save(pl_loc + '/FLIGHTS')

        logging.info('FLIGHTS has been loaded in the Presentation layer')

    except Exception as e:
        logging.error('Failed to load FLIGHTS in the Presentation Layer')
        spark.stop()
        raise Exception(f'Failed to load FLIGHTS in the Presentation Layer,{e}')


if __name__ == '__main__':
    spark = initialize_spark_session('load_pl_flights')
    from delta.tables import *

    yearmonth = None
    # finding and parsing the yearmonth argument for flights table partition
    for arg in sys.argv:
        if 'yearmonth' in arg:
            yearmonth = arg.split('=')[1]

    if yearmonth is not None:
        presentation_layer_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')
        integration_layer_name = dict_dbs_names.get('INTEGRATION_LAYER_NAME')

        load_pl_flights(spark,presentation_layer_loc, integration_layer_name, yearmonth)

    else:
        raise Exception('year month yyyymm argument is required to start loading, ex: yearmonth=202006')


