ddl_create_land_zone_db = """CREATE DATABASE IF NOT EXISTS {landing_zone_db_name} LOCATION '{landing_zone_db_loc}'"""
ddl_drop_land_zone_db = """DROP DATABASE IF EXISTS {landing_zone_db_name}"""

ddl_l_airline_id = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_AIRLINE_ID
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_AIRLINE_ID'
    OPTIONS ('header'='true')
"""

ddl_l_deparblk = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_DEPARRBLK
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_DEPARRBLK/'
    OPTIONS ('header'='true')
"""

ddl_l_state_fips = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_STATE_FIPS
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_STATE_FIPS/'
    OPTIONS ('header'='true')
"""

ddl_l_airport = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_AIRPORT
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_AIRPORT/'
    OPTIONS ('header'='true')
"""

ddl_l_airport_id = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_AIRPORT_ID
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_AIRPORT_ID/'
    OPTIONS ('header'='true')
"""

ddl_l_airport_seq_id = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_AIRPORT_SEQ_ID
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_AIRPORT_SEQ_ID/'
    OPTIONS ('header'='true')
"""

ddl_l_cancellation = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_CANCELLATION
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_CANCELLATION/'
    OPTIONS ('header'='true')
"""

ddl_l_months = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_MONTHS
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_MONTHS/'
    OPTIONS ('header'='true')
"""

ddl_l_world_area_codes = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_WORLD_AREA_CODES
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_WORLD_AREA_CODES/'
    OPTIONS ('header'='true')
"""

ddl_l_weekdays = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_WEEKDAYS
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_WEEKDAYS/'
    OPTIONS ('header'='true')
"""

ddl_l_diversions = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_DIVERSIONS
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_DIVERSIONS/'
    OPTIONS ('header'='true')
"""

ddl_l_distance_group_250 = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_DISTANCE_GROUP_250
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_DISTANCE_GROUP_250/'
    OPTIONS ('header'='true')
"""

ddl_l_unique_carriers = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_UNIQUE_CARRIERS
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_UNIQUE_CARRIERS/'
    OPTIONS ('header'='true')
"""

ddl_l_state_abr_aviation = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_STATE_ABR_AVIATION
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_STATE_ABR_AVIATION/'
    OPTIONS ('header'='true')
"""

ddl_l_city_market_id = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_CITY_MARKET_ID
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_CITY_MARKET_ID/'
    OPTIONS ('header'='true')
"""

ddl_l_ontime_delay_groups = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_ONTIME_DELAY_GROUPS
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_ONTIME_DELAY_GROUPS/'
    OPTIONS ('header'='true')
"""

ddl_l_yesno_resp = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_YESNO_RESP
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_YESNO_RESP/'
    OPTIONS ('header'='true')
"""

ddl_l_carrier_history = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_CARRIER_HISTORY
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_CARRIER_HISTORY/'
    OPTIONS ('header'='true')
"""

ddl_l_quarters = """
    CREATE TABLE IF NOT EXISTS {landing_zone_db_name}.L_QUARTERS
    (
        Code INTEGER,
        Description STRING
    )
    USING CSV
    LOCATION '{landing_zone_db_loc}/L_QUARTERS/'
    OPTIONS ('header'='true')
"""

dict_landing_zone_ddls = {
    'L_AIRLINE_ID': ddl_l_airline_id,
    'L_DEPARBLK': ddl_l_deparblk,
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
}
