from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import col
from sql_queries.sql_constants import dict_dbs_locations, dict_dbs_names
from sql_queries.sql_constants import missing_val_replace_alphanumeric, missing_val_replace_numeric


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
