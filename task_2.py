import pyspark.sql.functions as f
from settings import *
import read_write
def task_2(spark_session):
    """
    solution of task 2
    :param spark_session: spark session id
    :return:

    """
    f_name = path_dir_in + '/' + 'name.basics.tsv.gz'
    f_result = path_dir_out + '/' + 'task2'
    name_basics = read_write.read_imdb(spark_session, f_name, schema_name_basics)
    df_query2 = name_basics.select('primaryName', 'birthYear').where((f.col('birthYear') >= 1800) & (f.col('birthYear') < 1900))
    read_write.write_result(f_result, df_query2)