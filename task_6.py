import pyspark.sql.functions as f
from settings import *
import read_write
def task_6(spark_session):
    """
    solution of task 6
    :param spark_session: spark session id
    :return:

    """
    f_title_episode = path_dir_in + '/' + 'title.episode.tsv.gz'
    f_title_basics = path_dir_in + '/' + 'title.basics.tsv.gz'
    f_result = path_dir_out + '/' + 'task6'
    title_episode = read_write.read_imdb(spark_session, f_title_episode, schema_title_episode)
    title_basics = read_write.read_imdb(spark_session, f_title_basics, schema_title_basics)
    df_query6_1 = title_basics.select('tconst', 'primaryTitle').where(f.col('titleType') == 'tvSeries')
    df_query6_2 = df_query6_1.join(title_episode, title_episode.parentTconst == df_query6_1.tconst, how='inner')
    df_query6_f = df_query6_2.groupby('primaryTitle').count().orderBy('count', ascending=False).limit(100)
    read_write.write_result(f_result, df_query6_f)