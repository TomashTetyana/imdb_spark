import pyspark.sql.functions as f
from settings import *
import read_write
def task_1(spark_session):
    """
    solution of task 1
    :param spark_session: spark session id
    :return:

    """
    f_name = path_dir_in + '/' + 'title.akas.tsv.gz'
    f_result = path_dir_out + '/' + 'task1'
    title_akas = read_write.read_imdb(spark_session, f_name, schema_title_akas)
    df_query1 = title_akas.select('title').where(f.col('region') == 'UA')
    read_write.write_result(f_result,df_query1)