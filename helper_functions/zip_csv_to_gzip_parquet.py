import pandas as pd
import os


def zip_csv_to_gzip_parquet(file_path):
    """
    Function to read compresses CSV File into a pandas dataframe then transforms the data into parquet with GZIP compression to be suitable for spark
    Finally the src file is Deleted, this should be changed to archiving not deletion
    """
    # TODO replace Deletion part with arching strategy

    df = pd.read_csv(file_path, compression='zip',low_memory=False)

    # Dropping 'Unnamed: 109' column as it is an extra column exists in the src files with empty value and makes parquet files unreadable
    df.drop('Unnamed: 109',axis=1,inplace=True)

    # getting file year and file month to rename the output file
    file_year = str(df.loc[0]['YEAR'])
    file_month = str(df.loc[0]['MONTH']).rjust(2, '0')  # padding '0' for single number months

    saving_path = os.path.join('/'.join(file_path.split('/')[:-1]), file_year + file_month + '.gz')

    df.to_parquet(saving_path, 'pyarrow', compression='gzip')

    os.remove(file_path)
