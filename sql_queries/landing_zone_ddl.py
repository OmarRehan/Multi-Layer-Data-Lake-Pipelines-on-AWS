ddl_create_land_zone_db = """CREATE DATABASE IF NOT EXISTS {landing_zone_db_name} LOCATION '{landing_zone_db_loc}'"""
ddl_drop_land_zone_db = """DROP DATABASE IF EXISTS {landing_zone_db_name}"""

ddl_l_airline_id = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_AIRLINE_ID
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_AIRLINE_ID'
    OPTIONS ('header'='true')
"""

ddl_l_deparrblk = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_DEPARRBLK
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_DEPARRBLK/'
    OPTIONS ('header'='true')
"""

ddl_l_state_fips = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_STATE_FIPS
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_STATE_FIPS/'
    OPTIONS ('header'='true')
"""

ddl_l_airport = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_AIRPORT
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_AIRPORT/'
    OPTIONS ('header'='true')
"""

ddl_l_airport_id = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_AIRPORT_ID
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_AIRPORT_ID/'
    OPTIONS ('header'='true')
"""

ddl_l_airport_seq_id = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_AIRPORT_SEQ_ID
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_AIRPORT_SEQ_ID/'
    OPTIONS ('header'='true')
"""

ddl_l_cancellation = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_CANCELLATION
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_CANCELLATION/'
    OPTIONS ('header'='true')
"""

ddl_l_months = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_MONTHS
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_MONTHS/'
    OPTIONS ('header'='true')
"""

ddl_l_world_area_codes = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_WORLD_AREA_CODES
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_WORLD_AREA_CODES/'
    OPTIONS ('header'='true')
"""

ddl_l_weekdays = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_WEEKDAYS
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_WEEKDAYS/'
    OPTIONS ('header'='true')
"""

ddl_l_diversions = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_DIVERSIONS
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_DIVERSIONS/'
    OPTIONS ('header'='true')
"""

ddl_l_distance_group_250 = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_DISTANCE_GROUP_250
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_DISTANCE_GROUP_250/'
    OPTIONS ('header'='true')
"""

ddl_l_unique_carriers = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_UNIQUE_CARRIERS
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_UNIQUE_CARRIERS/'
    OPTIONS ('header'='true')
"""

ddl_l_state_abr_aviation = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_STATE_ABR_AVIATION
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_STATE_ABR_AVIATION/'
    OPTIONS ('header'='true')
"""

ddl_l_city_market_id = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_CITY_MARKET_ID
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_CITY_MARKET_ID/'
    OPTIONS ('header'='true')
"""

ddl_l_ontime_delay_groups = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_ONTIME_DELAY_GROUPS
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_ONTIME_DELAY_GROUPS/'
    OPTIONS ('header'='true')
"""

ddl_l_yesno_resp = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_YESNO_RESP
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_YESNO_RESP/'
    OPTIONS ('header'='true')
"""

ddl_l_carrier_history = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_CARRIER_HISTORY
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_CARRIER_HISTORY/'
    OPTIONS ('header'='true')
"""

ddl_l_quarters = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_QUARTERS
    (
        Code STRING,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_QUARTERS/'
    OPTIONS ('header'='true')
"""

ddl_flights = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.FLIGHTS
    (
        YEAR LONG,
        QUARTER LONG,
        MONTH LONG,
        DAY_OF_MONTH LONG,
        DAY_OF_WEEK LONG,
        FL_DATE STRING,
        OP_UNIQUE_CARRIER STRING,
        OP_CARRIER_AIRLINE_ID LONG,
        OP_CARRIER STRING,
        TAIL_NUM STRING,
        OP_CARRIER_FL_NUM LONG,
        ORIGIN_AIRPORT_ID LONG,
        ORIGIN_AIRPORT_SEQ_ID LONG,
        ORIGIN_CITY_MARKET_ID LONG,
        ORIGIN STRING,
        ORIGIN_CITY_NAME STRING,
        ORIGIN_STATE_ABR STRING,
        ORIGIN_STATE_FIPS LONG,
        ORIGIN_STATE_NM STRING,
        ORIGIN_WAC LONG,
        DEST_AIRPORT_ID LONG,
        DEST_AIRPORT_SEQ_ID LONG,
        DEST_CITY_MARKET_ID LONG,
        DEST STRING,
        DEST_CITY_NAME STRING,
        DEST_STATE_ABR STRING,
        DEST_STATE_FIPS LONG,
        DEST_STATE_NM STRING,
        DEST_WAC LONG,
        CRS_DEP_TIME LONG,
        DEP_TIME DOUBLE,
        DEP_DELAY DOUBLE,
        DEP_DELAY_NEW DOUBLE,
        DEP_DEL15 DOUBLE,
        DEP_DELAY_GROUP DOUBLE,
        DEP_TIME_BLK STRING,
        TAXI_OUT DOUBLE,
        WHEELS_OFF DOUBLE,
        WHEELS_ON DOUBLE,
        TAXI_IN DOUBLE,
        CRS_ARR_TIME LONG,
        ARR_TIME DOUBLE,
        ARR_DELAY DOUBLE,
        ARR_DELAY_NEW DOUBLE,
        ARR_DEL15 DOUBLE,
        ARR_DELAY_GROUP DOUBLE,
        ARR_TIME_BLK STRING,
        CANCELLED DOUBLE,
        CANCELLATION_CODE STRING,
        DIVERTED DOUBLE,
        CRS_ELAPSED_TIME DOUBLE,
        ACTUAL_ELAPSED_TIME DOUBLE,
        AIR_TIME DOUBLE,
        FLIGHTS DOUBLE,
        DISTANCE DOUBLE,
        DISTANCE_GROUP LONG,
        CARRIER_DELAY DOUBLE,
        WEATHER_DELAY DOUBLE,
        NAS_DELAY DOUBLE,
        SECURITY_DELAY DOUBLE,
        LATE_AIRCRAFT_DELAY DOUBLE,
        FIRST_DEP_TIME DOUBLE,
        TOTAL_ADD_GTIME DOUBLE,
        LONGEST_ADD_GTIME DOUBLE,
        DIV_AIRPORT_LANDINGS LONG,
        DIV_REACHED_DEST DOUBLE,
        DIV_ACTUAL_ELAPSED_TIME DOUBLE,
        DIV_ARR_DELAY DOUBLE,
        DIV_DISTANCE DOUBLE,
        DIV1_AIRPORT STRING,
        DIV1_AIRPORT_ID DOUBLE,
        DIV1_AIRPORT_SEQ_ID DOUBLE,
        DIV1_WHEELS_ON DOUBLE,
        DIV1_TOTAL_GTIME DOUBLE,
        DIV1_LONGEST_GTIME DOUBLE,
        DIV1_WHEELS_OFF DOUBLE,
        DIV1_TAIL_NUM STRING,
        DIV2_AIRPORT STRING,
        DIV2_AIRPORT_ID DOUBLE,
        DIV2_AIRPORT_SEQ_ID DOUBLE,
        DIV2_WHEELS_ON DOUBLE,
        DIV2_TOTAL_GTIME DOUBLE,
        DIV2_LONGEST_GTIME DOUBLE,
        DIV2_WHEELS_OFF DOUBLE,
        DIV2_TAIL_NUM STRING,
        DIV3_AIRPORT STRING,
        DIV3_AIRPORT_ID DOUBLE,
        DIV3_AIRPORT_SEQ_ID DOUBLE,
        DIV3_WHEELS_ON DOUBLE,
        DIV3_TOTAL_GTIME DOUBLE,
        DIV3_LONGEST_GTIME DOUBLE,
        DIV3_WHEELS_OFF DOUBLE,
        DIV3_TAIL_NUM STRING,
        DIV4_AIRPORT STRING,
        DIV4_AIRPORT_ID DOUBLE,
        DIV4_AIRPORT_SEQ_ID DOUBLE,
        DIV4_WHEELS_ON DOUBLE,
        DIV4_TOTAL_GTIME DOUBLE,
        DIV4_LONGEST_GTIME DOUBLE,
        DIV4_WHEELS_OFF DOUBLE,
        DIV4_TAIL_NUM STRING,
        DIV5_AIRPORT STRING,
        DIV5_AIRPORT_ID DOUBLE,
        DIV5_AIRPORT_SEQ_ID DOUBLE,
        DIV5_WHEELS_ON DOUBLE,
        DIV5_TOTAL_GTIME DOUBLE,
        DIV5_LONGEST_GTIME DOUBLE,
        DIV5_WHEELS_OFF DOUBLE,
        DIV5_TAIL_NUM STRING
    )
    USING PARQUET
    OPTIONS ('compression'='gzip')
    LOCATION '{landing_zone_db_loc}/FLIGHTS/'
"""

ddl_city_demographics = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.CITY_DEMOGRAPHICS
    (
        COUNT INTEGER,
        CITY STRING,
        NUMBER_OF_VETERANS DOUBLE,
        MALE_POPULATION DOUBLE,
        FOREIGN_BORN DOUBLE,
        AVERAGE_HOUSEHOLD_SIZE DOUBLE,
        MEDIAN_AGE DOUBLE,
        STATE STRING,
        RACE STRING,
        TOTAL_POPULATION INTEGER,
        STATE_CODE STRING,
        FEMALE_POPULATION DOUBLE
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/CITY_DEMOGRAPHICS/'
    OPTIONS ('header'='true')
"""

dict_landing_zone_ddls = {
    'L_AIRLINE_ID': ddl_l_airline_id,
    'L_DEPARRBLK': ddl_l_deparrblk,
    'L_STATE_FIPS': ddl_l_state_fips,
    'L_AIRPORT': ddl_l_airport,
    'L_AIRPORT_ID': ddl_l_airport_id,
    'L_AIRPORT_SEQ_ID': ddl_l_airport_seq_id,
    'L_CANCELLATION': ddl_l_cancellation,
    'L_MONTHS': ddl_l_months,
    'L_WORLD_AREA_CODES': ddl_l_world_area_codes,
    'L_WEEKDAYS': ddl_l_weekdays,
    'L_DIVERSIONS': ddl_l_diversions,
    'L_DISTANCE_GROUP_250': ddl_l_distance_group_250,
    'L_UNIQUE_CARRIERS': ddl_l_unique_carriers,
    'L_STATE_ABR_AVIATION': ddl_l_state_abr_aviation,
    'L_CITY_MARKET_ID': ddl_l_city_market_id,
    'L_ONTIME_DELAY_GROUPS': ddl_l_ontime_delay_groups,
    'L_YESNO_RESP': ddl_l_yesno_resp,
    'L_CARRIER_HISTORY': ddl_l_carrier_history,
    'L_QUARTERS': ddl_l_quarters,
    'FLIGHTS': ddl_flights,
    'CITY_DEMOGRAPHICS': ddl_city_demographics
}

list_landing_zone_standard_lookups = [
    'L_AIRLINE_ID',
    'L_DEPARRBLK',
    'L_STATE_FIPS',
    'L_AIRPORT',
    'L_AIRPORT_ID',
    'L_AIRPORT_SEQ_ID',
    'L_CANCELLATION',
    'L_MONTHS',
    'L_WORLD_AREA_CODES',
    'L_WEEKDAYS',
    'L_DIVERSIONS',
    'L_DISTANCE_GROUP_250',
    'L_UNIQUE_CARRIERS',
    'L_STATE_ABR_AVIATION',
    'L_CITY_MARKET_ID',
    'L_ONTIME_DELAY_GROUPS',
    'L_YESNO_RESP',
    'L_CARRIER_HISTORY',
    'L_QUARTERS'
]

list_landing_zone_facts = [
    'FLIGHTS'
]
