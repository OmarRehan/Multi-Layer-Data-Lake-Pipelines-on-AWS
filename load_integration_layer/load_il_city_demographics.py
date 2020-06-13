from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import col
from sql_queries.sql_constants import dict_dbs_locations, dict_dbs_names
from sql_queries.sql_constants import missing_val_replace_numeric


def load_city_demographics(spark, integration_layer_loc, landing_zone_name):
    delta_city_demographics = DeltaTable.forPath(spark, integration_layer_loc + '/CITY_DEMOGRAPHICS')

    df_LZ_l_airline_id = spark.sql(f"""
        SELECT 
        COUNT,
        CITY,
        NVL(NUMBER_OF_VETERANS,{missing_val_replace_numeric}) NUMBER_OF_VETERANS,
        NVL(MALE_POPULATION,{missing_val_replace_numeric}) MALE_POPULATION,
        NVL(FOREIGN_BORN,{missing_val_replace_numeric}) FOREIGN_BORN,
        NVL(AVERAGE_HOUSEHOLD_SIZE,{missing_val_replace_numeric}) AVERAGE_HOUSEHOLD_SIZE,
        MEDIAN_AGE,
        STATE,
        RACE,
        TOTAL_POPULATION,
        STATE_CODE,
        NVL(FEMALE_POPULATION,{missing_val_replace_numeric}) FEMALE_POPULATION
        FROM {landing_zone_name}.CITY_DEMOGRAPHICS
    """)

    delta_city_demographics.alias("oldData") \
        .merge(df_LZ_l_airline_id.alias("newData"), "oldData.CITY = newData.CITY AND oldData.STATE = newData.STATE AND oldData.RACE = newData.RACE") \
        .whenMatchedUpdate(set={
            "COUNT": col("newData.COUNT"),
            "NUMBER_OF_VETERANS": col("newData.NUMBER_OF_VETERANS"),
            "MALE_POPULATION": col("newData.MALE_POPULATION"),
            "FOREIGN_BORN": col("newData.FOREIGN_BORN"),
            "AVERAGE_HOUSEHOLD_SIZE": col("newData.AVERAGE_HOUSEHOLD_SIZE"),
            "MEDIAN_AGE": col("newData.MEDIAN_AGE"),
            "TOTAL_POPULATION": col("newData.TOTAL_POPULATION"),
            "STATE_CODE": col("newData.STATE_CODE"),
            "FEMALE_POPULATION": col("newData.FEMALE_POPULATION")
        }) \
        .whenNotMatchedInsert(values={
            "COUNT": col("newData.COUNT"),
            "CITY": col("newData.CITY"),
            "NUMBER_OF_VETERANS": col("newData.NUMBER_OF_VETERANS"),
            "MALE_POPULATION": col("newData.MALE_POPULATION"),
            "FOREIGN_BORN": col("newData.FOREIGN_BORN"),
            "AVERAGE_HOUSEHOLD_SIZE": col("newData.AVERAGE_HOUSEHOLD_SIZE"),
            "MEDIAN_AGE": col("newData.MEDIAN_AGE"),
            "STATE": col("newData.STATE"),
            "RACE": col("newData.RACE"),
            "TOTAL_POPULATION": col("newData.TOTAL_POPULATION"),
            "STATE_CODE": col("newData.STATE_CODE"),
            "FEMALE_POPULATION": col("newData.FEMALE_POPULATION")
        }) \
        .execute()


if __name__ == '__main__':
    spark = initialize_spark_session('load_il_load_city_demographics')
    from delta.tables import *

    integration_layer_loc = dict_dbs_locations.get('INTEGRATION_LAYER_LOC')
    landing_zone_name = dict_dbs_names.get('LANDING_ZONE_NAME')

    load_city_demographics(spark, integration_layer_loc, landing_zone_name)
