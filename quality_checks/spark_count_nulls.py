import logging
import sys
from helper_functions.initialize_spark_session import initialize_spark_session
from pyspark.sql.functions import isnan, count, isnull, col, when


def spark_count_nulls(spark, schehma_name, table_name, query_args=''):
    select_query = f"""SELECT * FROM {schehma_name}.{table_name} """

    if len(query_args) > 0:
        select_query = select_query + f""" WHERE {query_args}"""

    spark_df = spark.sql(select_query)

    pd_df_nulls = spark_df.select(
        [count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in spark_df.columns]).toPandas().T

    pd_df_nulls.reset_index(inplace=True)

    pd_df_nulls.rename(columns={'index': 'COLUMN_NAME', 0: 'NULL_COUNT'}, inplace=True)

    df_null_columns = pd_df_nulls[pd_df_nulls.NULL_COUNT != 0]

    if len(df_null_columns) > 0:
        raise Exception(
            f'{df_null_columns} NULLs exist in {schehma_name}.{table_name}\n{df_null_columns.to_string()}')
    else:
        logging.info(pd_df_nulls.to_string())


if __name__ == '__main__':
    spark = initialize_spark_session('spark_count_nulls')
    from delta.tables import *

    schehma_name = None
    table_name = None
    query_args = None

    # finding and parsing the yearmonth argument for flights table partition
    for arg in sys.argv:
        if 'schehma_name' in arg:
            schehma_name = arg.split('=')[1]
        if 'table_name' in arg:
            table_name = arg.split('=')[1]
        if 'query_args' in arg:
            query_args = arg.split('=',maxsplit=1)[1]

    if schehma_name is not None and table_name is not None:

        spark_count_nulls(spark, schehma_name, table_name, query_args)

    else:
        raise Exception('year month yyyymm argument is required to start loading, ex: schehma_name=PRESENTATION_LAYER table_name=FLIGHTS query_args=FLIGHT_YEARMON=201901')
