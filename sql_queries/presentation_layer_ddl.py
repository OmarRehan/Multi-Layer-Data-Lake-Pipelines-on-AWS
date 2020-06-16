ddl_create_presentation_layer_db = """CREATE DATABASE IF NOT EXISTS {presentation_layer_db_name} LOCATION '{presentation_layer_db_loc}'"""
ddl_drop_presentation_layer_db = """DROP DATABASE IF EXISTS {presentation_layer_db_name} CASCADE"""

##################################################
# Schema for each table in the integration layer #
##################################################
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, DoubleType, DateType

schema_cancellation = StructType(
    [
        StructField('CODE', StringType(), True),
        StructField('DESCRIPTION', StringType(), True)
    ]
)

schema_world_area_codes = StructType(
    [
        StructField('CODE', IntegerType(), True),
        StructField('NAME', StringType(), True)
    ]
)

schema_state = StructType(
    [
        StructField('STATE_ABR', StringType(), True),
        StructField('STATE_FIPS', IntegerType(), True),
        StructField('STATE_NAME', StringType(), True),
        StructField('WAC_CODE', IntegerType(), True)
    ]
)

schema_city = StructType(
    [
        StructField('CITY_ID', LongType(), True),
        StructField('CITY_NAME', StringType(), True),
        StructField('STATE_ABR', StringType(), True)
    ]
)

schema_airport = StructType(
    [
        StructField('AIRPORT_CODE', IntegerType(), True),
        StructField('AIRPORT_NAME', StringType(), True),
        StructField('CITY_ID', LongType(), True)
    ]
)

schema_airline = StructType(
    [
        StructField("AIRLINE_ID", IntegerType(), True),
        StructField("AIRLINE_NAME", StringType(), True),
        StructField("AIRLINE_CODE", StringType(), True)
    ]
)

schema_calendar = StructType(
    [
        StructField("DATE_ID", IntegerType(), True),
        StructField("DATE_COL", DateType(), True),
        StructField("YEAR", IntegerType(), True),
        StructField("MONTH", IntegerType(), True),
        StructField("DAY", IntegerType(), True),
        StructField("DAY_OF_MONTH", IntegerType(), True),
        StructField("DAY_OF_YEAR", IntegerType(), True),
        StructField("DAY_NAME", StringType(), True)
    ]
)

schema_city_demographics = StructType(
    [
        StructField("CITY_ID", LongType(), True),
        StructField("RACE", StringType(), True),
        StructField("NUMBER_OF_VETERANS", IntegerType(), True),
        StructField("MALE_POPULATION", IntegerType(), True),
        StructField('FEMALE_POPULATION',IntegerType(), True),
        StructField("FOREIGN_BORN", IntegerType(), True),
        StructField("AVERAGE_HOUSEHOLD_SIZE", DoubleType(), True),
        StructField("MEDIAN_AGE", DoubleType(), True),
        StructField("TOTAL_POPULATION", IntegerType(), True)
    ]
)


schema_flights = StructType(
    [
        StructField("FLIGHT_ID", StringType(), True),
        StructField("FLIGHT_DATE", IntegerType(), True),
        StructField("AIRLINE_ID", LongType(), True),
        StructField("TAIL_NUM", StringType(), True),
        StructField('FLIGHT_NUM',LongType(), True),
        StructField("ORIGIN_AIRPORT_ID", LongType(), True),
        StructField("DEST_AIRPORT_ID", LongType(), True),
        StructField("DEPARTURE_TIME", StringType(), True),
        StructField("EARLY_DEPARTURE_MINS", IntegerType(), True),
        StructField("DELAY_DEPARTURE_MINS", IntegerType(), True),
        StructField("TAXI_OUT_MINS", DoubleType(), True),
        StructField("TAXI_IN_MINS", DoubleType(), True),
        StructField("WHEELS_OFF_TIME", StringType(), True),
        StructField('WHEELS_ON_TIME', StringType(), True),
        StructField("ARR_TIME", StringType(), True),
        StructField("EARLY_ARRIVAL_MINS", IntegerType(), True),
        StructField("DELAY_ARRIVAL_MINS", IntegerType(), True),
        StructField("CANCELLATION_CODE", StringType(), True),
        StructField("FLIGHT_ELAPSED_TIME", DoubleType(), True),
        StructField("AIR_TIME", DoubleType(), True),
        StructField("DISTANCE_MILES", DoubleType(), True),
        StructField("CARRIER_DELAY_MINS", IntegerType(), True),
        StructField('WEATHER_DELAY_MINS', IntegerType(), True),
        StructField("NAS_DELAY_MINS", IntegerType(), True),
        StructField("SECURITY_DELAY_MINS", IntegerType(), True),
        StructField("LATE_AIRCRAFT_DELAY_MINS", IntegerType(), True),
        StructField("TOTAL_ADD_GTIME_MINS", IntegerType(), True),
        StructField("FLIGHT_YEARMON", IntegerType(), True)
    ]
)

# Contains all presentation layer's non partitioned tables
dict_pl_non_partitioned_tables = {
    'CANCELLATION': schema_cancellation,
    'WORLD_AREA_CODES': schema_world_area_codes,
    'STATE': schema_state,
    'CITY': schema_city,
    'AIRPORT': schema_airport,
    'AIRLINE': schema_airline,
    'CITY_DEMOGRAPHICS': schema_city_demographics
}
