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
