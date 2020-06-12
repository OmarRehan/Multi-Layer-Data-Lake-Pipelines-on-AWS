from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, DoubleType

ddl_create_integration_layer_db = """CREATE DATABASE IF NOT EXISTS {integration_layer_db_name} LOCATION '{integration_layer_db_loc}'"""
ddl_drop_integration_layer_db = """DROP DATABASE IF EXISTS {integration_layer_db_name} CASCADE"""

# In Integration layer i am using delta lake as storage layer to get its features like ACID, Data versions, Merge etc..., in delta lake we have to define spark schema to create a table
# , so here i will define spark schemas compatible to all integration layer tables

##################################################
# Schema for each table in the integration layer #
##################################################

schema_l_airline_id = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_airport = StructType(
    [
        StructField("Code", StringType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_airport_id = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_airport_seq_id = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_cancellation = StructType(
    [
        StructField("Code", StringType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_carrier_history = StructType(
    [
        StructField("Code", StringType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_city_market_id = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_deparrblk = StructType(
    [
        StructField("Code", StringType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_distance_group_250 = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_diversions = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_months = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_ontime_delay_groups = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_quarters = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_state_abr_aviation = StructType(
    [
        StructField("Code", StringType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_state_fips = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_unique_carriers = StructType(
    [
        StructField("Code", StringType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_weekdays = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_world_area_codes = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_l_yesno_resp = StructType(
    [
        StructField("Code", IntegerType(), True),
        StructField("Description", StringType(), True)
    ]
)

schema_flights = StructType(
    [
        StructField("YEAR", LongType(), True),
        StructField("QUARTER", LongType(), True),
        StructField("MONTH", LongType(), True),
        StructField("DAY_OF_MONTH", LongType(), True),
        StructField("DAY_OF_WEEK", LongType(), True),
        StructField("FL_DATE", StringType(), True),
        StructField("OP_UNIQUE_CARRIER", StringType(), True),
        StructField("OP_CARRIER_AIRLINE_ID", LongType(), True),
        StructField("OP_CARRIER", StringType(), True),
        StructField("TAIL_NUM", StringType(), True),
        StructField("OP_CARRIER_FL_NUM", LongType(), True),
        StructField("ORIGIN_AIRPORT_ID", LongType(), True),
        StructField("ORIGIN_AIRPORT_SEQ_ID", LongType(), True),
        StructField("ORIGIN_CITY_MARKET_ID", LongType(), True),
        StructField("ORIGIN", StringType(), True),
        StructField("ORIGIN_CITY_NAME", StringType(), True),
        StructField("ORIGIN_STATE_ABR", StringType(), True),
        StructField("ORIGIN_STATE_FIPS", LongType(), True),
        StructField("ORIGIN_STATE_NM", StringType(), True),
        StructField("ORIGIN_WAC", LongType(), True),
        StructField("DEST_AIRPORT_ID", LongType(), True),
        StructField("DEST_AIRPORT_SEQ_ID", LongType(), True),
        StructField("DEST_CITY_MARKET_ID", LongType(), True),
        StructField("DEST", StringType(), True),
        StructField("DEST_CITY_NAME", StringType(), True),
        StructField("DEST_STATE_ABR", StringType(), True),
        StructField("DEST_STATE_FIPS", LongType(), True),
        StructField("DEST_STATE_NM", StringType(), True),
        StructField("DEST_WAC", LongType(), True),
        StructField("CRS_DEP_TIME", LongType(), True),
        StructField("DEP_TIME", DoubleType(), True),
        StructField("DEP_DELAY", DoubleType(), True),
        StructField("DEP_DELAY_NEW", DoubleType(), True),
        StructField("DEP_DEL15", DoubleType(), True),
        StructField("DEP_DELAY_GROUP", DoubleType(), True),
        StructField("DEP_TIME_BLK", StringType(), True),
        StructField("TAXI_OUT", DoubleType(), True),
        StructField("WHEELS_OFF", DoubleType(), True),
        StructField("WHEELS_ON", DoubleType(), True),
        StructField("TAXI_IN", DoubleType(), True),
        StructField("CRS_ARR_TIME", LongType(), True),
        StructField("ARR_TIME", DoubleType(), True),
        StructField("ARR_DELAY", DoubleType(), True),
        StructField("ARR_DELAY_NEW", DoubleType(), True),
        StructField("ARR_DEL15", DoubleType(), True),
        StructField("ARR_DELAY_GROUP", DoubleType(), True),
        StructField("ARR_TIME_BLK", StringType(), True),
        StructField("CANCELLED", DoubleType(), True),
        StructField("CANCELLATION_CODE", StringType(), True),
        StructField("DIVERTED", DoubleType(), True),
        StructField("CRS_ELAPSED_TIME", DoubleType(), True),
        StructField("ACTUAL_ELAPSED_TIME", DoubleType(), True),
        StructField("AIR_TIME", DoubleType(), True),
        StructField("FLIGHTS", DoubleType(), True),
        StructField("DISTANCE", DoubleType(), True),
        StructField("DISTANCE_GROUP", LongType(), True),
        StructField("CARRIER_DELAY", DoubleType(), True),
        StructField("WEATHER_DELAY", DoubleType(), True),
        StructField("NAS_DELAY", DoubleType(), True),
        StructField("SECURITY_DELAY", DoubleType(), True),
        StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True),
        StructField("FIRST_DEP_TIME", DoubleType(), True),
        StructField("TOTAL_ADD_GTIME", DoubleType(), True),
        StructField("LONGEST_ADD_GTIME", DoubleType(), True),
        StructField("DIV_AIRPORT_LANDINGS", LongType(), True),
        StructField("DIV_REACHED_DEST", DoubleType(), True),
        StructField("DIV_ACTUAL_ELAPSED_TIME", DoubleType(), True),
        StructField("DIV_ARR_DELAY", DoubleType(), True),
        StructField("DIV_DISTANCE", DoubleType(), True),
        StructField("DIV1_AIRPORT", StringType(), True),
        StructField("DIV1_AIRPORT_ID", DoubleType(), True),
        StructField("DIV1_AIRPORT_SEQ_ID", DoubleType(), True),
        StructField("DIV1_WHEELS_ON", DoubleType(), True),
        StructField("DIV1_TOTAL_GTIME", DoubleType(), True),
        StructField("DIV1_LONGEST_GTIME", DoubleType(), True),
        StructField("DIV1_WHEELS_OFF", DoubleType(), True),
        StructField("DIV1_TAIL_NUM", StringType(), True),
        StructField("DIV2_AIRPORT", StringType(), True),
        StructField("DIV2_AIRPORT_ID", DoubleType(), True),
        StructField("DIV2_AIRPORT_SEQ_ID", DoubleType(), True),
        StructField("DIV2_WHEELS_ON", DoubleType(), True),
        StructField("DIV2_TOTAL_GTIME", DoubleType(), True),
        StructField("DIV2_LONGEST_GTIME", DoubleType(), True),
        StructField("DIV2_WHEELS_OFF", DoubleType(), True),
        StructField("DIV2_TAIL_NUM", StringType(), True),
        StructField("DIV3_AIRPORT", StringType(), True),
        StructField("DIV3_AIRPORT_ID", DoubleType(), True),
        StructField("DIV3_AIRPORT_SEQ_ID", DoubleType(), True),
        StructField("DIV3_WHEELS_ON", DoubleType(), True),
        StructField("DIV3_TOTAL_GTIME", DoubleType(), True),
        StructField("DIV3_LONGEST_GTIME", DoubleType(), True),
        StructField("DIV3_WHEELS_OFF", DoubleType(), True),
        StructField("DIV3_TAIL_NUM", DoubleType(), True),
        StructField("DIV4_AIRPORT", DoubleType(), True),
        StructField("DIV4_AIRPORT_ID", DoubleType(), True),
        StructField("DIV4_AIRPORT_SEQ_ID", DoubleType(), True),
        StructField("DIV4_WHEELS_ON", DoubleType(), True),
        StructField("DIV4_TOTAL_GTIME", DoubleType(), True),
        StructField("DIV4_LONGEST_GTIME", DoubleType(), True),
        StructField("DIV4_WHEELS_OFF", DoubleType(), True),
        StructField("DIV4_TAIL_NUM", DoubleType(), True),
        StructField("DIV5_AIRPORT", DoubleType(), True),
        StructField("DIV5_AIRPORT_ID", DoubleType(), True),
        StructField("DIV5_AIRPORT_SEQ_ID", DoubleType(), True),
        StructField("DIV5_WHEELS_ON", DoubleType(), True),
        StructField("DIV5_TOTAL_GTIME", DoubleType(), True),
        StructField("DIV5_LONGEST_GTIME", DoubleType(), True),
        StructField("DIV5_WHEELS_OFF", DoubleType(), True),
        StructField("DIV5_TAIL_NUM", DoubleType(), True)
    ]
)

dict_integration_layer_standard_lookups = {
    'L_AIRLINE_ID': schema_l_airline_id,
    'L_DEPARRBLK': schema_l_deparrblk,
    'L_STATE_FIPS': schema_l_state_fips,
    'L_AIRPORT': schema_l_airport,
    'L_AIRPORT_ID': schema_l_airport_id,
    'L_AIRPORT_SEQ_ID': schema_l_airport_seq_id,
    'L_CANCELLATION': schema_l_cancellation,
    'L_MONTHS': schema_l_months,
    'L_WORLD_AREA_CODES': schema_l_world_area_codes,
    'L_WEEKDAYS': schema_l_weekdays,
    'L_DIVERSIONS': schema_l_diversions,
    'L_DISTANCE_GROUP_250': schema_l_distance_group_250,
    'L_UNIQUE_CARRIERS': schema_l_unique_carriers,
    'L_STATE_ABR_AVIATION': schema_l_state_abr_aviation,
    'L_CITY_MARKET_ID': schema_l_city_market_id,
    'L_ONTIME_DELAY_GROUPS': schema_l_ontime_delay_groups,
    'L_YESNO_RESP': schema_l_yesno_resp,
    'L_CARRIER_HISTORY': schema_l_carrier_history,
    'L_QUARTERS': schema_l_quarters
}

list_integration_layer_facts = [
    'FLIGHTS'
]
