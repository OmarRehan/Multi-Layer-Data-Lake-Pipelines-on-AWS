from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import col
from sql_queries.sql_constants import dict_dbs_locations, dict_dbs_names
from sql_queries.sql_constants import missing_val_replace_alphanumeric,missing_val_replace_numeric


def load_l_airline_id(spark, integration_layer_loc, landing_zone_name):
    delta_l_airline_id = DeltaTable.forPath(spark, integration_layer_loc + '/L_AIRLINE_ID')

    df_LZ_l_airline_id = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_AIRLINE_ID
    """)

    delta_l_airline_id.alias("oldData") \
        .merge(df_LZ_l_airline_id.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_airport(spark, integration_layer_loc, landing_zone_name):
    delta_l_airport = DeltaTable.forPath(spark, integration_layer_loc + '/L_AIRPORT')

    df_LZ_l_airport = spark.sql(f"""
        SELECT 
        CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_AIRPORT
    """)

    delta_l_airport.alias("oldData") \
        .merge(df_LZ_l_airport.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_airport_id(spark, integration_layer_loc, landing_zone_name):
    delta_l_airport_id = DeltaTable.forPath(spark, integration_layer_loc + '/L_AIRPORT_ID')

    df_LZ_l_airport_id = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_AIRPORT_ID
    """)

    delta_l_airport_id.alias("oldData") \
        .merge(df_LZ_l_airport_id.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_airport_seq_id(spark, integration_layer_loc, landing_zone_name):
    delta_l_airport_seq_id = DeltaTable.forPath(spark, integration_layer_loc + '/L_AIRPORT_SEQ_ID')

    df_LZ_l_airport_seq_id = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_AIRPORT_SEQ_ID
    """)

    delta_l_airport_seq_id.alias("oldData") \
        .merge(df_LZ_l_airport_seq_id.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_cancellation(spark, integration_layer_loc, landing_zone_name):
    delta_l_cancellation = DeltaTable.forPath(spark, integration_layer_loc + '/L_CANCELLATION')

    df_LZ_l_cancellation = spark.sql(f"""
        SELECT 
        CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_CANCELLATION
    """)

    delta_l_cancellation.alias("oldData") \
        .merge(df_LZ_l_cancellation.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_carrier_history(spark, integration_layer_loc, landing_zone_name):
    delta_l_carrier_history = DeltaTable.forPath(spark, integration_layer_loc + '/L_CARRIER_HISTORY')

    df_LZ_l_carrier_history = spark.sql(f"""
        SELECT 
        CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_CARRIER_HISTORY
        WHERE CODE IS NOT NULL
    """)

    delta_l_carrier_history.alias("oldData") \
        .merge(df_LZ_l_carrier_history.alias("newData"),
               "oldData.CODE = newData.CODE AND oldData.DESCRIPTION = newData.DESCRIPTION") \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_city_market(spark, integration_layer_loc, landing_zone_name):
    delta_l_city_market = DeltaTable.forPath(spark, integration_layer_loc + '/L_CITY_MARKET_ID')

    df_LZ_l_city_market = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_CITY_MARKET_ID
    """)

    delta_l_city_market.alias("oldData") \
        .merge(df_LZ_l_city_market.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_deparrblk(spark, integration_layer_loc, landing_zone_name):
    delta_l_deparrblk = DeltaTable.forPath(spark, integration_layer_loc + '/L_DEPARRBLK')

    df_LZ_l_deparrblk = spark.sql(f"""
        SELECT 
        CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_DEPARRBLK
    """)

    delta_l_deparrblk.alias("oldData") \
        .merge(df_LZ_l_deparrblk.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_distance_group_250(spark, integration_layer_loc, landing_zone_name):
    delta_l_distance_group_250 = DeltaTable.forPath(spark, integration_layer_loc + '/L_DISTANCE_GROUP_250')

    df_LZ_l_distance_group_250 = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_DISTANCE_GROUP_250
    """)

    delta_l_distance_group_250.alias("oldData") \
        .merge(df_LZ_l_distance_group_250.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_diversions(spark, integration_layer_loc, landing_zone_name):
    delta_l_diversions = DeltaTable.forPath(spark, integration_layer_loc + '/L_DIVERSIONS')

    df_LZ_l_diversions = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_DIVERSIONS
    """)

    delta_l_diversions.alias("oldData") \
        .merge(df_LZ_l_diversions.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_months(spark, integration_layer_loc, landing_zone_name):
    delta_l_months = DeltaTable.forPath(spark, integration_layer_loc + '/L_MONTHS')

    df_LZ_l_months = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_MONTHS
    """)

    delta_l_months.alias("oldData") \
        .merge(df_LZ_l_months.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_ontime_delay_groups(spark, integration_layer_loc, landing_zone_name):
    delta_l_ontime_delay_groups = DeltaTable.forPath(spark, integration_layer_loc + '/L_ONTIME_DELAY_GROUPS')

    df_LZ_l_ontime_delay_groups = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_ONTIME_DELAY_GROUPS
    """)

    delta_l_ontime_delay_groups.alias("oldData") \
        .merge(df_LZ_l_ontime_delay_groups.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_ontime_delay_groups(spark, integration_layer_loc, landing_zone_name):
    delta_l_ontime_delay_groups = DeltaTable.forPath(spark, integration_layer_loc + '/L_ONTIME_DELAY_GROUPS')

    df_LZ_l_ontime_delay_groups = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_ONTIME_DELAY_GROUPS
    """)

    delta_l_ontime_delay_groups.alias("oldData") \
        .merge(df_LZ_l_ontime_delay_groups.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_quarters(spark, integration_layer_loc, landing_zone_name):
    delta_l_quarters = DeltaTable.forPath(spark, integration_layer_loc + '/L_QUARTERS')

    df_LZ_l_quarters = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_QUARTERS
    """)

    delta_l_quarters.alias("oldData") \
        .merge(df_LZ_l_quarters.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_state_abr_aviation(spark, integration_layer_loc, landing_zone_name):
    delta_l_state_abr_aviation = DeltaTable.forPath(spark, integration_layer_loc + '/L_STATE_ABR_AVIATION')

    df_LZ_l_state_abr_aviation = spark.sql(f"""
        SELECT 
        CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_STATE_ABR_AVIATION
    """)

    delta_l_state_abr_aviation.alias("oldData") \
        .merge(df_LZ_l_state_abr_aviation.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_state_fips(spark, integration_layer_loc, landing_zone_name):
    delta_l_state_state_fips = DeltaTable.forPath(spark, integration_layer_loc + '/L_STATE_FIPS')

    df_LZ_l_state_state_fips = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_STATE_FIPS
    """)

    delta_l_state_state_fips.alias("oldData") \
        .merge(df_LZ_l_state_state_fips.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_unique_carriers(spark, integration_layer_loc, landing_zone_name):
    delta_l_unique_carriers = DeltaTable.forPath(spark, integration_layer_loc + '/L_UNIQUE_CARRIERS')

    df_LZ_l_unique_carriers = spark.sql(f"""
        SELECT 
        CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_UNIQUE_CARRIERS
    """)

    delta_l_unique_carriers.alias("oldData") \
        .merge(df_LZ_l_unique_carriers.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_weekdays(spark, integration_layer_loc, landing_zone_name):
    delta_l_weekdays = DeltaTable.forPath(spark, integration_layer_loc + '/L_WEEKDAYS')

    df_LZ_l_weekdays = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_WEEKDAYS
    """)

    delta_l_weekdays.alias("oldData") \
        .merge(df_LZ_l_weekdays.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_world_area_codes(spark, integration_layer_loc, landing_zone_name):
    delta_l_world_area_codes = DeltaTable.forPath(spark, integration_layer_loc + '/L_WORLD_AREA_CODES')

    df_LZ_l_wworld_area_codes = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_WORLD_AREA_CODES
    """)

    delta_l_world_area_codes.alias("oldData") \
        .merge(df_LZ_l_wworld_area_codes.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_l_yesno_resp(spark, integration_layer_loc, landing_zone_name):
    delta_l_yesno_resp = DeltaTable.forPath(spark, integration_layer_loc + '/L_YESNO_RESP')

    df_LZ_l_yesno_resp = spark.sql(f"""
        SELECT 
        CAST(CODE AS INTEGER) AS CODE
        ,DESCRIPTION
        FROM {landing_zone_name}.L_YESNO_RESP
    """)

    delta_l_yesno_resp.alias("oldData") \
        .merge(df_LZ_l_yesno_resp.alias("newData"), "oldData.CODE = newData.CODE") \
        .whenMatchedUpdate(set={"DESCRIPTION": col("newData.DESCRIPTION")}) \
        .whenNotMatchedInsert(values={"CODE": col("newData.CODE"), "DESCRIPTION": col("newData.DESCRIPTION")}) \
        .execute()


def load_flights(spark, integration_layer_loc, landing_zone_name):
    delta_flights = DeltaTable.forPath(spark, integration_layer_loc + '/FLIGHTS')

    delta_flights.delete()

    df_LZ_flights = spark.sql(f"""
        SELECT
        YEAR,
        QUARTER,
        MONTH,
        DAY_OF_MONTH,
        DAY_OF_WEEK,
        FL_DATE,
        OP_UNIQUE_CARRIER,
        OP_CARRIER_AIRLINE_ID,
        OP_CARRIER,
        NVL(TAIL_NUM,'{missing_val_replace_alphanumeric}') TAIL_NUM,
        OP_CARRIER_FL_NUM,
        ORIGIN_AIRPORT_ID,
        ORIGIN_AIRPORT_SEQ_ID,
        ORIGIN_CITY_MARKET_ID,
        ORIGIN,
        ORIGIN_CITY_NAME,
        ORIGIN_STATE_ABR,
        ORIGIN_STATE_FIPS,
        ORIGIN_STATE_NM,
        ORIGIN_WAC,
        DEST_AIRPORT_ID,
        DEST_AIRPORT_SEQ_ID,
        DEST_CITY_MARKET_ID,
        DEST,
        DEST_CITY_NAME,
        DEST_STATE_ABR,
        DEST_STATE_FIPS,
        DEST_STATE_NM,
        DEST_WAC,
        CRS_DEP_TIME,
        NVL(DEP_TIME,{missing_val_replace_numeric}) DEP_TIME,
        NVL(DEP_DELAY,{missing_val_replace_numeric}) DEP_DELAY,
        NVL(DEP_DELAY_NEW,{missing_val_replace_numeric}) DEP_DELAY_NEW,
        NVL(DEP_DEL15,{missing_val_replace_numeric}) DEP_DEL15,
        NVL(DEP_DELAY_GROUP,{missing_val_replace_numeric}) DEP_DELAY_GROUP,
        DEP_TIME_BLK,
        NVL(TAXI_OUT,{missing_val_replace_numeric}) TAXI_OUT,
        NVL(WHEELS_OFF,{missing_val_replace_numeric}) WHEELS_OFF,
        NVL(WHEELS_ON,{missing_val_replace_numeric}) WHEELS_ON,
        NVL(TAXI_IN,{missing_val_replace_numeric}) TAXI_IN,
        CRS_ARR_TIME,
        NVL(ARR_TIME,{missing_val_replace_numeric}) ARR_TIME,
        NVL(ARR_DELAY,{missing_val_replace_numeric}) ARR_DELAY,
        NVL(ARR_DELAY_NEW,{missing_val_replace_numeric}) ARR_DELAY_NEW,
        NVL(ARR_DEL15,{missing_val_replace_numeric}) ARR_DEL15,
        NVL(ARR_DELAY_GROUP,{missing_val_replace_numeric}) ARR_DELAY_GROUP,
        ARR_TIME_BLK,
        CANCELLED,
        NVL(CANCELLATION_CODE,'{missing_val_replace_alphanumeric}') CANCELLATION_CODE,
        DIVERTED,
        NVL(CRS_ELAPSED_TIME,{missing_val_replace_numeric}) CRS_ELAPSED_TIME,
        NVL(ACTUAL_ELAPSED_TIME,{missing_val_replace_numeric}) ACTUAL_ELAPSED_TIME,
        NVL(AIR_TIME,{missing_val_replace_numeric}) AIR_TIME,
        FLIGHTS,
        DISTANCE,
        DISTANCE_GROUP,
        NVL(CARRIER_DELAY,{missing_val_replace_numeric}) CARRIER_DELAY,
        NVL(WEATHER_DELAY,{missing_val_replace_numeric}) WEATHER_DELAY,
        NVL(NAS_DELAY,{missing_val_replace_numeric}) NAS_DELAY,
        NVL(SECURITY_DELAY,{missing_val_replace_numeric}) SECURITY_DELAY,
        NVL(LATE_AIRCRAFT_DELAY,{missing_val_replace_numeric}) LATE_AIRCRAFT_DELAY,
        NVL(FIRST_DEP_TIME,{missing_val_replace_numeric}) FIRST_DEP_TIME,
        NVL(TOTAL_ADD_GTIME,{missing_val_replace_numeric}) TOTAL_ADD_GTIME,
        NVL(LONGEST_ADD_GTIME,{missing_val_replace_numeric}) LONGEST_ADD_GTIME,
        DIV_AIRPORT_LANDINGS,
        NVL(DIV_REACHED_DEST,{missing_val_replace_numeric}) DIV_REACHED_DEST,
        NVL(DIV_ACTUAL_ELAPSED_TIME,{missing_val_replace_numeric}) DIV_ACTUAL_ELAPSED_TIME,
        NVL(DIV_ARR_DELAY,{missing_val_replace_numeric}) DIV_ARR_DELAY,
        NVL(DIV_DISTANCE,{missing_val_replace_numeric}) DIV_DISTANCE,
        NVL(DIV1_AIRPORT,'{missing_val_replace_alphanumeric}') DIV1_AIRPORT,
        NVL(DIV1_AIRPORT_ID,{missing_val_replace_numeric}) DIV1_AIRPORT_ID,
        NVL(DIV1_AIRPORT_SEQ_ID,{missing_val_replace_numeric}) DIV1_AIRPORT_SEQ_ID,
        NVL(DIV1_WHEELS_ON,{missing_val_replace_numeric}) DIV1_WHEELS_ON,
        NVL(DIV1_TOTAL_GTIME,{missing_val_replace_numeric}) DIV1_TOTAL_GTIME,
        NVL(DIV1_LONGEST_GTIME,{missing_val_replace_numeric}) DIV1_LONGEST_GTIME,
        NVL(DIV1_WHEELS_OFF,{missing_val_replace_numeric}) DIV1_WHEELS_OFF,
        NVL(DIV1_TAIL_NUM,'{missing_val_replace_alphanumeric}') DIV1_TAIL_NUM,
        NVL(DIV2_AIRPORT,'{missing_val_replace_alphanumeric}') DIV2_AIRPORT,
        NVL(DIV2_AIRPORT_ID,{missing_val_replace_numeric}) DIV2_AIRPORT_ID,
        NVL(DIV2_AIRPORT_SEQ_ID,{missing_val_replace_numeric}) DIV2_AIRPORT_SEQ_ID,
        NVL(DIV2_WHEELS_ON,{missing_val_replace_numeric}) DIV2_WHEELS_ON,
        NVL(DIV2_TOTAL_GTIME,{missing_val_replace_numeric}) DIV2_TOTAL_GTIME,
        NVL(DIV2_LONGEST_GTIME,{missing_val_replace_numeric}) DIV2_LONGEST_GTIME,
        NVL(DIV2_WHEELS_OFF,{missing_val_replace_numeric}) DIV2_WHEELS_OFF,
        NVL(DIV2_TAIL_NUM,'{missing_val_replace_alphanumeric}') DIV2_TAIL_NUM,
        NVL(DIV3_AIRPORT,'{missing_val_replace_alphanumeric}') DIV3_AIRPORT,
        NVL(DIV3_AIRPORT_ID,{missing_val_replace_numeric}) DIV3_AIRPORT_ID,
        NVL(DIV3_AIRPORT_SEQ_ID,{missing_val_replace_numeric}) DIV3_AIRPORT_SEQ_ID,
        NVL(DIV3_WHEELS_ON,{missing_val_replace_numeric}) DIV3_WHEELS_ON,
        NVL(DIV3_TOTAL_GTIME,{missing_val_replace_numeric}) DIV3_TOTAL_GTIME,
        NVL(DIV3_LONGEST_GTIME,{missing_val_replace_numeric}) DIV3_LONGEST_GTIME,
        NVL(DIV3_WHEELS_OFF,{missing_val_replace_numeric}) DIV3_WHEELS_OFF,
        NVL(DIV3_TAIL_NUM,{missing_val_replace_numeric}) DIV3_TAIL_NUM,
        NVL(DIV4_AIRPORT,{missing_val_replace_numeric}) DIV4_AIRPORT,
        NVL(DIV4_AIRPORT_ID,{missing_val_replace_numeric}) DIV4_AIRPORT_ID,
        NVL(DIV4_AIRPORT_SEQ_ID,{missing_val_replace_numeric}) DIV4_AIRPORT_SEQ_ID,
        NVL(DIV4_WHEELS_ON,{missing_val_replace_numeric}) DIV4_WHEELS_ON,
        NVL(DIV4_TOTAL_GTIME,{missing_val_replace_numeric}) DIV4_TOTAL_GTIME,
        NVL(DIV4_LONGEST_GTIME,{missing_val_replace_numeric}) DIV4_LONGEST_GTIME,
        NVL(DIV4_WHEELS_OFF,{missing_val_replace_numeric}) DIV4_WHEELS_OFF,
        NVL(DIV4_TAIL_NUM,{missing_val_replace_numeric}) DIV4_TAIL_NUM,
        NVL(DIV5_AIRPORT,{missing_val_replace_numeric}) DIV5_AIRPORT,
        NVL(DIV5_AIRPORT_ID,{missing_val_replace_numeric}) DIV5_AIRPORT_ID,
        NVL(DIV5_AIRPORT_SEQ_ID,{missing_val_replace_numeric}) DIV5_AIRPORT_SEQ_ID,
        NVL(DIV5_WHEELS_ON,{missing_val_replace_numeric}) DIV5_WHEELS_ON,
        NVL(DIV5_TOTAL_GTIME,{missing_val_replace_numeric}) DIV5_TOTAL_GTIME,
        NVL(DIV5_LONGEST_GTIME,{missing_val_replace_numeric}) DIV5_LONGEST_GTIME,
        NVL(DIV5_WHEELS_OFF,{missing_val_replace_numeric}) DIV5_WHEELS_OFF,
        NVL(DIV5_TAIL_NUM,{missing_val_replace_numeric}) DIV5_TAIL_NUM
        FROM {landing_zone_name}.FLIGHTS
    """)

    df_LZ_flights.write.format("delta").mode("append").save(integration_layer_loc+'/FLIGHTS')


if __name__ == '__main__':
    spark = initialize_spark_session('load_integration_layer')
    from delta.tables import *

    integration_layer_loc = dict_dbs_locations.get('INTEGRATION_LAYER_LOC')
    landing_zone_name = dict_dbs_names.get('LANDING_ZONE_NAME')

    load_l_airline_id(spark, integration_layer_loc, landing_zone_name)
    load_l_airport(spark, integration_layer_loc, landing_zone_name)
    load_l_airport_id(spark, integration_layer_loc, landing_zone_name)
    load_l_airport_seq_id(spark, integration_layer_loc, landing_zone_name)
    load_l_cancellation(spark, integration_layer_loc, landing_zone_name)
    load_l_carrier_history(spark, integration_layer_loc, landing_zone_name)
    load_l_city_market(spark, integration_layer_loc, landing_zone_name)
    load_l_deparrblk(spark, integration_layer_loc, landing_zone_name)
    load_l_distance_group_250(spark, integration_layer_loc, landing_zone_name)
    load_l_diversions(spark, integration_layer_loc, landing_zone_name)
    load_l_months(spark, integration_layer_loc, landing_zone_name)
    load_l_ontime_delay_groups(spark, integration_layer_loc, landing_zone_name)
    load_l_quarters(spark, integration_layer_loc, landing_zone_name)
    load_l_state_abr_aviation(spark, integration_layer_loc, landing_zone_name)
    load_l_state_fips(spark, integration_layer_loc, landing_zone_name)
    load_l_unique_carriers(spark, integration_layer_loc, landing_zone_name)
    load_l_weekdays(spark, integration_layer_loc, landing_zone_name)
    load_l_world_area_codes(spark, integration_layer_loc, landing_zone_name)
    load_l_yesno_resp(spark, integration_layer_loc, landing_zone_name)
    load_flights(spark, integration_layer_loc, landing_zone_name)
