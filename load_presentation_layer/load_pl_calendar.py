import logging
import sys
from helper_functions.initialize_spark_session import initialize_spark_session
from constants import dict_dbs_locations


def generate_calendar(start_date, end_date):
    try:

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
    except Exception as e:
        logging.error('Failed to generate CALENDAR data')
        raise Exception(f'Failed to generate CALENDAR data,{e}')


def load_pl_calendar(spark, pl_loc, start_date, end_date):
    try:
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

        logging.info('CALENDAR has been loaded in the Presentation layer')

    except Exception as e:
        logging.error('Failed to load CALENDAR in the Presentation Layer')
        spark.stop()
        raise Exception(f'Failed to load CALENDAR in the Presentation Layer,{e}')


if __name__ == '__main__':

    start_date = None
    end_date = None
    # finding and parsing start date & end date args to be provided to the function
    for arg in sys.argv:
        if 'start_date' in arg:
            start_date = arg.split('=')[1]
        elif 'end_date' in arg:
            end_date = arg.split('=')[1]

    if (start_date is not None) and (end_date is not None):

        spark = initialize_spark_session('load_pl_calendar')
        from delta.tables import *

        presentation_layer_loc = dict_dbs_locations.get('PRESENTATION_LAYER_LOC')

        load_pl_calendar(spark, presentation_layer_loc, start_date, end_date)

    else:
        raise Exception(
            'start_date and end_date arguments are required to start loading, ex: start_date=2020-01-01 end_date=2022-12-31')
