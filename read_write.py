import os
def read_imdb(session, f_in, schema_df):
    df_in = session.read.csv(f_in, header = True, nullValue = 'null', schema = schema_df, sep='\t')
    return df_in

def write_result(f_out, df):
    df.write.csv(f_out, header = True, mode = 'overwrite')
