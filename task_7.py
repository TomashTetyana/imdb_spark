import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from settings import *
import read_write
def task_7(spark_session):
    """
    solution of task 7
    :param spark_session: spark session id
    :return:

    """
    f_title_ratings = path_dir_in + '/' + 'title.ratings.tsv.gz'
    f_title_basics = path_dir_in + '/' + 'title.basics.tsv.gz'
    f_result = path_dir_out + '/' + 'task7'
    title_ratings = read_write.read_imdb(spark_session, f_title_ratings, schema_title_ratings)
    title_basics = read_write.read_imdb(spark_session, f_title_basics, schema_title_basics)
    df_query7_1 = (title_basics.withColumn('decade',
                                           f.array(f.floor(f.col('startYear') / 10) * 10,
                                                   (f.when(f.col('endYear').isNotNull(),
                                                           (f.ceil(f.col('endYear') / 10) * 10 - 1))
                                                    .otherwise((f.floor(f.col('startYear') / 10) * 10 + 9))))))
    df_query7_2 = df_query7_1.select('tconst', 'primaryTitle', 'decade')
    df_query7_3 = df_query7_2.join(title_ratings, df_query7_2.tconst == title_ratings.tconst, 'inner')
    window_dept = Window.partitionBy("decade").orderBy(f.col("averageRating").desc())
    df_query7_4 = df_query7_3.withColumn("top", f.row_number().over(window_dept))
    df_query7_5 = df_query7_4.select('primaryTitle', 'decade', 'averageRating', 'numVotes', 'top').filter(
        f.col('top') <= 10)
    df_query7_f = df_query7_5.withColumn('decade', df_query7_5.decade.cast(t.StringType()))
    read_write.write_result(f_result, df_query7_f)

