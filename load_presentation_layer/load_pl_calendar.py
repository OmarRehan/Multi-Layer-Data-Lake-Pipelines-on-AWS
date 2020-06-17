from helper_functions.initialize_spark_session import initialize_spark_session
from constants import dict_dbs_locations


def generate_calendar(start_date, end_date):
    import pandas as pd
    df = pd.DataFrame({'DATE': pd.date_range(start_date, end_date)})

    from pyspark.sql.functions import year, month, date_format, expr, dayofyear, dayofmonth

    df_spark = spark.createDataFrame(df)

    df_spark = df_spark.withColumn('DATE_ID', expr("REPLACE(DATE,'-','') DATE"))

    df_spark = df_spark.withColumn('DATE_ID', expr("CAST(REPLACE(CAST(DATE AS DATE),'-','') AS INTEGER)")) \
        .withColumn('DATE_COL', expr("CAST(DATE AS DATE)")) \
        .withColumn('YEAR', year('DATE')) \
        .withColumn('MONTH', month('DATE')) \
        .withColumn('DAY', dayofmonth('DATE')) \
        .withColumn('DAY_OF_MONTH', dayofmonth('DATE')) \
        .withColumn('DAY_OF_YEAR', dayofyear('DATE')) \
        .withColumn('DAY_NAME', date_format('DATE', 'E')) \
        .drop('DATE')

    return df_spark


# TODO : use Args as input for the function
def load_pl_calendar(pl_loc, start_date, end_date):
    delta_pl_calendar = generate_calendar(start_date, end_date)

    start_year = int(start_date[0:4])
    start_month = int(start_date[5:7])

    end_year = int(end_date[0:4])
    end_month = int(end_date[5:7])

    delta_pl_calendar.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere",
                f"YEAR >= {start_year} AND MONTH >= {start_month} AND YEAR <= {end_year} AND MONTH <= {end_month}") \
        .save(pl_loc + '/CALENDAR')


if __name__ == '__main__':
    spark = initialize_spark_session('load_pl_calendar')
    from delta.tables import *

    presentation_layer_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')

    load_pl_calendar(presentation_layer_loc, '2029-01-01', '2030-12-31')
