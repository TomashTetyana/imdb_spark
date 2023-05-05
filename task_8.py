import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from settings import *
import read_write
def task_8(spark_session):
    """
    solution of task 8
    :param spark_session: spark session id
    :return:

    """
    f_title_ratings = path_dir_in + '/' + 'title.ratings.tsv.gz'
    f_title_basics = path_dir_in + '/' + 'title.basics.tsv.gz'
    f_result = path_dir_out + '/' + 'task8'
    title_ratings = read_write.read_imdb(spark_session, f_title_ratings, schema_title_ratings)
    title_basics = read_write.read_imdb(spark_session, f_title_basics, schema_title_basics)
    df_query8_1 = title_basics.select('tconst', 'primaryTitle', 'genres').filter(f.col('genres') != '\\N')
    df_query8_2 = df_query8_1.join(title_ratings, df_query8_1.tconst == title_ratings.tconst, 'inner')
    window_dept = Window.partitionBy("genres").orderBy(f.col("averageRating").desc())
    df_query8_3 = df_query8_2.withColumn("top", f.row_number().over(window_dept))
    df_query8_f = df_query8_3.select('primaryTitle', 'genres', 'averageRating', 'numVotes', 'top').filter(
        f.col('top') <= 10)
    read_write.write_result(f_result, df_query8_f)