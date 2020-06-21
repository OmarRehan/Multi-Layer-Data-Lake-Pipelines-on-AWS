import logging
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

    df = pd.read_csv(file_path, compression='zip', low_memory=False)

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
