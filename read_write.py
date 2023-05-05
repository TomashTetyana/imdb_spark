import os
def read_imdb(session, f_in, schema_df):
    '''
     the function of reading a dataframe from a file
    :param session: session ID
    :param f_in: file with data
    :param schema_df: data schema
    :return: DataFrame
    '''
    df_in = session.read.csv(f_in, header = True, nullValue = 'null', schema = schema_df, sep='\t')
    return df_in

def write_result(f_out, df):
    '''
     the function of writing a data frame to a file
    :param f_out: file for result
    :param df: DataFrame
    :return:
    '''
    df.write.csv(f_out, header = True, mode = 'overwrite')
