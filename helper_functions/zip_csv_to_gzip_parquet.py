import logging

import numpy as np
import pandas as pd
import os
from constants import edge_node_path
from helper_functions.loop_files import loop_files

logging.basicConfig(level=logging.INFO,format="%(asctime)s:%(levelname)s:%(message)s")


def zip_csv_to_gzip_parquet(file_path):
    """
    Function to read compressed CSV File into a pandas dataframe then transforms the data into parquet with GZIP compression to be suitable for spark
    Finally the src file is Deleted, this should be changed to archiving not deletion
    """
    # TODO replace Deletion part with archiving strategy

    dtypes = {
        'YEAR': np.dtype(np.int64),
        'QUARTER': np.dtype(np.int64),
        'MONTH': np.dtype(np.int64),
        'DAY_OF_MONTH': np.dtype(np.int64),
        'DAY_OF_WEEK': np.dtype(np.int64),
        'FL_DATE': np.dtype(np.object),
        'OP_UNIQUE_CARRIER': np.dtype(np.object),
        'OP_CARRIER_AIRLINE_ID': np.dtype(np.int64),
        'OP_CARRIER': np.dtype(np.object),
        'TAIL_NUM': np.dtype(np.object),
        'OP_CARRIER_FL_NUM': np.dtype(np.int64),
        'ORIGIN_AIRPORT_ID': np.dtype(np.int64),
        'ORIGIN_AIRPORT_SEQ_ID': np.dtype(np.int64),
        'ORIGIN_CITY_MARKET_ID': np.dtype(np.int64),
        'ORIGIN': np.dtype(np.object),
        'ORIGIN_CITY_NAME': np.dtype(np.object),
        'ORIGIN_STATE_ABR': np.dtype(np.object),
        'ORIGIN_STATE_FIPS': np.dtype(np.int64),
        'ORIGIN_STATE_NM': np.dtype(np.object),
        'ORIGIN_WAC': np.dtype(np.int64),
        'DEST_AIRPORT_ID': np.dtype(np.int64),
        'DEST_AIRPORT_SEQ_ID': np.dtype(np.int64),
        'DEST_CITY_MARKET_ID': np.dtype(np.int64),
        'DEST': np.dtype(np.object),
        'DEST_CITY_NAME': np.dtype(np.object),
        'DEST_STATE_ABR': np.dtype(np.object),
        'DEST_STATE_FIPS': np.dtype(np.int64),
        'DEST_STATE_NM': np.dtype(np.object),
        'DEST_WAC': np.dtype(np.int64),
        'CRS_DEP_TIME': np.dtype(np.int64),
        'DEP_TIME': np.dtype(np.float64),
        'DEP_DELAY': np.dtype(np.float64),
        'DEP_DELAY_NEW': np.dtype(np.float64),
        'DEP_DEL15': np.dtype(np.float64),
        'DEP_DELAY_GROUP': np.dtype(np.float64),
        'DEP_TIME_BLK': np.dtype(np.object),
        'TAXI_OUT': np.dtype(np.float64),
        'WHEELS_OFF': np.dtype(np.float64),
        'WHEELS_ON': np.dtype(np.float64),
        'TAXI_IN': np.dtype(np.float64),
        'CRS_ARR_TIME': np.dtype(np.int64),
        'ARR_TIME': np.dtype(np.float64),
        'ARR_DELAY': np.dtype(np.float64),
        'ARR_DELAY_NEW': np.dtype(np.float64),
        'ARR_DEL15': np.dtype(np.float64),
        'ARR_DELAY_GROUP': np.dtype(np.float64),
        'ARR_TIME_BLK': np.dtype(np.object),
        'CANCELLED': np.dtype(np.float64),
        'CANCELLATION_CODE': np.dtype(np.object),
        'DIVERTED': np.dtype(np.float64),
        'CRS_ELAPSED_TIME': np.dtype(np.float64),
        'ACTUAL_ELAPSED_TIME': np.dtype(np.float64),
        'AIR_TIME': np.dtype(np.float64),
        'FLIGHTS': np.dtype(np.float64),
        'DISTANCE': np.dtype(np.float64),
        'DISTANCE_GROUP': np.dtype(np.int64),
        'CARRIER_DELAY': np.dtype(np.float64),
        'WEATHER_DELAY': np.dtype(np.float64),
        'NAS_DELAY': np.dtype(np.float64),
        'SECURITY_DELAY': np.dtype(np.float64),
        'LATE_AIRCRAFT_DELAY': np.dtype(np.float64),
        'FIRST_DEP_TIME': np.dtype(np.float64),
        'TOTAL_ADD_GTIME': np.dtype(np.float64),
        'LONGEST_ADD_GTIME': np.dtype(np.float64),
        'DIV_AIRPORT_LANDINGS': np.dtype(np.int64),
        'DIV_REACHED_DEST': np.dtype(np.float64),
        'DIV_ACTUAL_ELAPSED_TIME': np.dtype(np.float64),
        'DIV_ARR_DELAY': np.dtype(np.float64),
        'DIV_DISTANCE': np.dtype(np.float64),
        'DIV1_AIRPORT': np.dtype(np.object),
        'DIV1_AIRPORT_ID': np.dtype(np.float64),
        'DIV1_AIRPORT_SEQ_ID': np.dtype(np.float64),
        'DIV1_WHEELS_ON': np.dtype(np.float64),
        'DIV1_TOTAL_GTIME': np.dtype(np.float64),
        'DIV1_LONGEST_GTIME': np.dtype(np.float64),
        'DIV1_WHEELS_OFF': np.dtype(np.float64),
        'DIV1_TAIL_NUM': np.dtype(np.object),
        'DIV2_AIRPORT': np.dtype(np.object),
        'DIV2_AIRPORT_ID': np.dtype(np.float64),
        'DIV2_AIRPORT_SEQ_ID': np.dtype(np.float64),
        'DIV2_WHEELS_ON': np.dtype(np.float64),
        'DIV2_TOTAL_GTIME': np.dtype(np.float64),
        'DIV2_LONGEST_GTIME': np.dtype(np.float64),
        'DIV2_WHEELS_OFF': np.dtype(np.float64),
        'DIV2_TAIL_NUM': np.dtype(np.object),
        'DIV3_AIRPORT': np.dtype(np.object),
        'DIV3_AIRPORT_ID': np.dtype(np.float64),
        'DIV3_AIRPORT_SEQ_ID': np.dtype(np.float64),
        'DIV3_WHEELS_ON': np.dtype(np.float64),
        'DIV3_TOTAL_GTIME': np.dtype(np.float64),
        'DIV3_LONGEST_GTIME': np.dtype(np.float64),
        'DIV3_WHEELS_OFF': np.dtype(np.float64),
        'DIV3_TAIL_NUM': np.dtype(np.object),
        'DIV4_AIRPORT': np.dtype(np.object),
        'DIV4_AIRPORT_ID': np.dtype(np.float64),
        'DIV4_AIRPORT_SEQ_ID': np.dtype(np.float64),
        'DIV4_WHEELS_ON': np.dtype(np.float64),
        'DIV4_TOTAL_GTIME': np.dtype(np.float64),
        'DIV4_LONGEST_GTIME': np.dtype(np.float64),
        'DIV4_WHEELS_OFF': np.dtype(np.float64),
        'DIV4_TAIL_NUM': np.dtype(np.object),
        'DIV5_AIRPORT': np.dtype(np.object),
        'DIV5_AIRPORT_ID': np.dtype(np.float64),
        'DIV5_AIRPORT_SEQ_ID': np.dtype(np.float64),
        'DIV5_WHEELS_ON': np.dtype(np.float64),
        'DIV5_TOTAL_GTIME': np.dtype(np.float64),
        'DIV5_LONGEST_GTIME': np.dtype(np.float64),
        'DIV5_WHEELS_OFF': np.dtype(np.float64),
        'DIV5_TAIL_NUM': np.dtype(np.object)
    }

    df = pd.read_csv(file_path, compression='zip', low_memory=False,dtype=dtypes)

    # Dropping 'Unnamed: 109' column as it is an extra column exists in the src files with empty value and makes parquet files unreadable
    df.drop('Unnamed: 109', axis=1, inplace=True)

    # getting file year and file month to rename the output file
    file_year = str(df.loc[0]['YEAR'])
    file_month = str(df.loc[0]['MONTH']).rjust(2, '0')  # padding '0' for single number months

    saving_path = os.path.join('/'.join(file_path.split('/')[:-1]), file_year + file_month + '.gz')

    df.to_parquet(saving_path, 'pyarrow', compression='gzip')

    os.remove(file_path)


if __name__ == '__main__':

    flights_table_name = 'FLIGHTS'

    # Looping over the zip files in the Edge Node directory
    list_zip_files = loop_files(os.path.join(edge_node_path, flights_table_name), '*.zip')

    # get total number of files found
    num_files = len(list_zip_files)
    logging.info(' {} files found in {}'.format(num_files, os.path.join(edge_node_path, flights_table_name)))

    # Transforming the available csv zip files into parquet gzip
    for file in list_zip_files:
        zip_csv_to_gzip_parquet(file)
        logging.info(f'{file} transformed into parquet gzip')
