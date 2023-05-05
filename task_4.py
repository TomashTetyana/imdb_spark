import pyspark.sql.functions as f
from settings import *
import read_write
def task_4(spark_session):
    """
    solution of task 4
    :param spark_session: spark session id
    :return:

    """
    f_name_basics = path_dir_in + '/' + 'name.basics.tsv.gz'
    f_title_basics = path_dir_in + '/' + 'title.basics.tsv.gz'
    f_title_principals = path_dir_in + '/' + 'title.principals.tsv.gz'
    f_result = path_dir_out + '/' + 'task4'
    name_basics = read_write.read_imdb(spark_session, f_name_basics, schema_name_basics)
    title_principals = read_write.read_imdb(spark_session, f_title_principals, schema_title_principals)
    title_basics = read_write.read_imdb(spark_session, f_title_basics, schema_title_basics)

    df_query4_1 = title_principals.drop('ordering', 'job').where(f.col('category') == 'actor')
    df_query4_2 = name_basics.select('nconst', 'primaryName')
    df_query4_3 = title_basics.select('tconst', 'primaryTitle').filter((f.col('titleType') == 'movie') |
                                                                       (f.col('titleType') == 'tvMovie') |
                                                                       (f.col('titleType') == 'tvSeries') |
                                                                       (f.col('titleType') == 'tvMiniSeries'))
    df_query4_4 = df_query4_1.join(df_query4_2, df_query4_1.nconst == df_query4_2.nconst, 'inner')
    df_query4_5 = df_query4_4.join(df_query4_3, df_query4_4.tconst == df_query4_3.tconst, 'inner')
    df_query4_f = df_query4_5.select('primaryName', 'primaryTitle', 'characters').filter(f.col('characters') != '\\N')
    read_write.write_result(f_result, df_query4_f)