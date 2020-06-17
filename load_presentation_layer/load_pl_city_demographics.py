from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import col
from constants import dict_dbs_locations, dict_dbs_names


def load_pl_city_demographics(spark, pl_name, pl_loc, il_name):
    delta_pl_city_demographics = DeltaTable.forPath(spark, pl_loc + '/CITY_DEMOGRAPHICS')

    df_il_city_demographics = spark.sql(f"""
        SELECT
        CITY.CITY_ID,
        TRIM(UPPER(RACE)) RACE,
        CAST(NUMBER_OF_VETERANS AS INTEGER) NUMBER_OF_VETERANS,
        CAST(MALE_POPULATION AS INTEGER) MALE_POPULATION,
        CAST(FEMALE_POPULATION AS INTEGER) FEMALE_POPULATION,
        CAST(FOREIGN_BORN AS INTEGER) FOREIGN_BORN,
        AVERAGE_HOUSEHOLD_SIZE,
        MEDIAN_AGE,
        CAST(TOTAL_POPULATION AS INTEGER) TOTAL_POPULATION
        FROM {il_name}.city_demographics
        INNER JOIN {pl_name}.CITY
        ON TRIM(UPPER(city_demographics.CITY)) = CITY.CITY_NAME
        AND city_demographics.STATE_CODE = CITY.STATE_ABR
    """)

    delta_pl_city_demographics.alias("oldData") \
        .merge(df_il_city_demographics.alias("newData"),
               "oldData.CITY_ID = newData.CITY_ID AND oldData.RACE = newData.RACE") \
        .whenMatchedUpdate(set={
            "NUMBER_OF_VETERANS": col("newData.NUMBER_OF_VETERANS"),
            "MALE_POPULATION": col("newData.MALE_POPULATION"),
            "FEMALE_POPULATION": col("newData.FEMALE_POPULATION"),
            "AVERAGE_HOUSEHOLD_SIZE": col("newData.AVERAGE_HOUSEHOLD_SIZE"),
            "MEDIAN_AGE": col("newData.MEDIAN_AGE"),
            "TOTAL_POPULATION": col("newData.TOTAL_POPULATION")
        }) \
        .whenNotMatchedInsert(values={
            "CITY_ID": col("newData.CITY_ID"),
            "RACE": col("newData.RACE"),
            "NUMBER_OF_VETERANS": col("newData.NUMBER_OF_VETERANS"),
            "MALE_POPULATION": col("newData.MALE_POPULATION"),
            "FEMALE_POPULATION": col("newData.FEMALE_POPULATION"),
            "AVERAGE_HOUSEHOLD_SIZE": col("newData.AVERAGE_HOUSEHOLD_SIZE"),
            "MEDIAN_AGE": col("newData.MEDIAN_AGE"),
            "TOTAL_POPULATION": col("newData.TOTAL_POPULATION")
        }) \
        .execute()


if __name__ == '__main__':
    spark = initialize_spark_session('load_pl_city_demographics')
    from delta.tables import *

    presentation_layer_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')
    presentation_layer_name = dict_dbs_names.get('PRESENTATION_LAYER_NAME')
    integration_layer_name = dict_dbs_names.get('INTEGRATION_LAYER_NAME')

    load_pl_city_demographics(spark, presentation_layer_name, presentation_layer_loc, integration_layer_name)
