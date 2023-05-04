import pyspark.sql.functions as f
from settings import *
import read_write
def task_3(spark_session):
    """
    solution of task 3
    :param spark_session: spark session id
    :return:

    """
    f_name = path_dir_in + '/' + 'title.basics.tsv.gz'
    f_result = path_dir_out + '/' + 'task3'

    title_basics = read_write.read_imdb(spark_session, f_name, schema_title_basics)
    df_query3 = title_basics.select('primaryTitle').where((f.col('titleType') == 'movie') & (f.col('runtimeMinutes') > 120))
    read_write.write_result(f_result, df_query3)