import pyspark.sql.functions as f
from settings import *
import read_write
def task_5(spark_session):
    """
    solution of task 5
    :param spark_session: spark session id
    :return:

    """
    f_title_akas = path_dir_in + '/' + 'title.akas.tsv.gz'
    f_title_basics = path_dir_in + '/' + 'title.basics.tsv.gz'
    f_result = path_dir_out + '/' + 'task5'
    title_akas = read_write.read_imdb(spark_session, f_title_akas, schema_title_akas)
    title_basics = read_write.read_imdb(spark_session, f_title_basics, schema_title_basics)

    df_query5_1 = title_akas.select('titleId', 'region')
    df_query5_2 = title_basics.select('tconst').where(f.col("isAdult") == 1)
    df_query5_3 = df_query5_2.join(df_query5_1, df_query5_1.titleId == df_query5_2.tconst, 'inner')
    df_query5_4 = df_query5_3.groupby('region').count().orderBy('count', ascending=False).limit(100)
    df_query5_5 = df_query5_3.groupby('region').count().orderBy('count', ascending=True).limit(100)
    df_query5_f = df_query5_4.union(df_query5_5)
    df_query5_f.show()
    read_write.write_result(f_result, df_query5_f)