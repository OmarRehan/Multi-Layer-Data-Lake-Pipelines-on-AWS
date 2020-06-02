import pandas as pd


def get_profile(df):
    """
    Used for Profiling DataFrames.

    returns : A DF with columns represents columns data types, total count fo all records, number of nulls, percentage of nulls & unique values

    TODO : Change the Null Column to be Invalid values and consider cheking empty string and white spaces
    """

    df_dtypes = df.dtypes.to_frame('COL_DTYPE')

    df_nullability = df.isnull().sum().to_frame('COL_NULLS')

    df_nullability['COL_NULL%'] = round((df_nullability['COL_NULLS'] / len(df)) * 100, 2)

    # joining between dtypes df with nullability df
    df_res = pd.merge(df_dtypes, df_nullability, how='inner', left_index=True, right_index=True)

    df_uniq_vals = pd.DataFrame(columns=['COL_NAME', 'UNIQ_VALS'])

    # counting unique values per column in the df
    for col in df.columns:
        df_uniq_vals.loc[len(df_uniq_vals)] = [col, str(len(df[col].value_counts()))]

    # joining the result df with unique values df
    df_res = pd.merge(df_res, df_uniq_vals, left_index=True, right_on='COL_NAME')

    # df length
    df_length = len(df)

    df_res['TOTAL_COUNT'] = df_length

    df_res.set_index('COL_NAME', drop=True, inplace=True)

    return df_res[['COL_DTYPE', 'TOTAL_COUNT', 'UNIQ_VALS', 'COL_NULLS', 'COL_NULL%']]
