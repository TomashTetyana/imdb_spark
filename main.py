import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

import pandas as pd
''''
conf = SparkConf()
conf.setMaster("local").setAppName('My app')
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
print('Запущен Spark версии', spark.version)

print(name_basics.describe())
name_basics.printSchema()
print(name_basics.show())
'''
def main():
   spark_session = (SparkSession.builder
                    .master("local")
                    .appName("task app")
                    .config(conf=SparkConf())
                    .getOrCreate())

   path_dir_in = r'../imdb-spark/data'
   path_dir_out=r'../imdb-spark/result'
   schema_name_basics = t.StructType([t.StructField("nconst", t.StringType(), False),
                                      t.StructField("primaryName", t.StringType(), True),
                                      t.StructField("birthYear", t.IntegerType(), True),
                                      t.StructField("deathYear", t.IntegerType(), True),
                                      t.StructField("primaryProfession", t.StringType(), True),
                                      t.StructField("knownForTitles", t.StringType(), True)])
   f_name = path_dir_in + '/' + 'name.basics.tsv.gz'
   name_basics = spark_session.read.csv(f_name, header=True, nullValue='null', schema=schema_name_basics, sep='\t')

   schema_title_akas = t.StructType([t.StructField("titleId", t.StringType(), False),
                                     t.StructField("ordering", t.IntegerType(), True),
                                     t.StructField("title", t.StringType(), True),
                                     t.StructField("region", t.StringType(), True),
                                     t.StructField("language", t.StringType(), True),
                                     t.StructField("types", t.StringType(), True),
                                     t.StructField("attributes", t.StringType(), True),
                                     t.StructField("isOriginalTitle", t.IntegerType(), True)])
   f_name = path_dir_in + '/' + 'title.akas.tsv.gz'
   title_akas = spark_session.read.csv(f_name, header=True, nullValue='null', schema=schema_title_akas, sep='\t')

   schema_title_basics = t.StructType([t.StructField("tconst", t.StringType(), False),
                                       t.StructField("titleType", t.StringType(), True),
                                       t.StructField("primaryTitle", t.StringType(), True),
                                       t.StructField("originalTitle", t.StringType(), True),
                                       t.StructField("isAdult", t.IntegerType(), True),
                                       t.StructField("startYear", t.IntegerType(), True),
                                       t.StructField("endYear", t.IntegerType(), True),
                                       t.StructField("runtimeMinutes", t.IntegerType(), True),
                                       t.StructField("genres", t.StringType(), True)])
   f_name = path_dir_in + '/' + 'title.basics.tsv.gz'
   title_basics = spark_session.read.csv(f_name, header=True, nullValue='null', schema=schema_title_basics, sep='\t')

   schema_title_principals = t.StructType([t.StructField("tconst", t.StringType(), False),
                                           t.StructField("ordering", t.IntegerType(), True),
                                           t.StructField("nconst", t.StringType(), True),
                                           t.StructField("category", t.StringType(), True),
                                           t.StructField("job", t.StringType(), True),
                                           t.StructField("characters", t.StringType(), True)])
   f_name = path_dir_in + '/' + 'title.principals.tsv.gz'
   title_principals = spark_session.read.csv(f_name, header=True, schema=schema_title_principals, sep='\t')

   schema_title_episode = t.StructType([t.StructField("tconst", t.StringType(), False),
                                        t.StructField("parentTconst", t.StringType(), True),
                                        t.StructField("seasonNumber", t.IntegerType(), True),
                                        t.StructField("episodeNumber", t.IntegerType(), True)])
   f_name = path_dir_in + '/' + 'title.episode.tsv.gz'
   title_episode = spark_session.read.csv(f_name, header=True, nullValue='null', schema=schema_title_episode,
                                             sep='\t')

   schema_title_ratings = t.StructType([t.StructField("tconst", t.StringType(), False),
                                                 t.StructField("averageRating", t.DoubleType(), True),
                                                 t.StructField("numVotes", t.IntegerType(), True)])
   f_name = path_dir_in + '/' + 'title.ratings.tsv.gz'
   title_ratings = spark_session.read.csv(f_name, header=True, nullValue='null', schema=schema_title_ratings,
                                          sep='\t')

   schema_title_crew = t.StructType([t.StructField("tconst", t.StringType(), False),
                                     t.StructField("directors", t.StringType(), True),
                                     t.StructField("writers", t.StringType(), True)])
   f_name = path_dir_in + '/' + 'title.crew.tsv.gz'
   title_crew = spark_session.read.csv(f_name, header=True, nullValue='null', schema=schema_title_crew,
                                          sep='\t')
   #print(title_crew.show())

   #df_query1 = title_akas.select("title").where(f.col("region") == "UA")
   #df_query2 = name_basics.select("primaryName", "birthYear").where((f.col("birthYear") >= 1800) & (f.col("birthYear") < 1900))
   #df_query3 = title_basics.select("primaryTitle").where((f.col("titleType") == 'movie') & (f.col("runtimeMinutes") > 120))
   '''
   df_query4_1 = title_principals.drop("ordering","job").where(f.col('category')=='actor')
   df_query4_2 = name_basics.select('nconst','primaryName')
   df_query4_3 = title_basics.select('tconst','primaryTitle').filter((f.col('titleType') == 'movie') |
                                                                     (f.col('titleType') == 'tvMovie') |
                                                                     (f.col('titleType') == 'tvSeries') |
                                                                     (f.col('titleType') == 'tvMiniSeries'))
   df_query4_4 = df_query4_1.join(df_query4_2,df_query4_1.nconst==df_query4_2.nconst,'inner')
   df_query4 = df_query4_4.join(df_query4_3, df_query4_4.tconst == df_query4_3.tconst, 'inner')
   dr_query4_f = df_query4.select('primaryName','primaryTitle','characters').filter(f.col('characters') != '\\N')
   dr_query4_f.show()
   
   df_query5_1 = title_akas.select('titleId','region')
   df_query5_2 = title_basics.select('tconst').where(f.col("isAdult") == 1)
   df_query5_3 = df_query5_2.join(df_query5_1, df_query5_1.titleId == df_query5_2.tconst, 'inner')
   df_query5_f = df_query5_3.groupby('region').count().orderBy('count', ascending=False).limit(100)
   df_query5_f.show()
   
   df_query6_1 = title_basics.select('tconst','primaryTitle').where(f.col('titleType') == 'tvSeries')
   df_query6_2 = df_query6_1.join(title_episode, title_episode.parentTconst == df_query6_1.tconst, how = 'inner')
   df_query6_f = df_query6_2.groupby('primaryTitle').count().orderBy('count', ascending=False).limit(100)
   df_query6_f.show()

   df_query7_1 = (title_basics.withColumn('decade',
                                          f.array(f.floor(f.col('startYear')/10)*10,
                                                  (f.when(f.col('endYear').isNotNull(),
                                                          (f.ceil(f.col('endYear')/10)*10-1))
                                                  .otherwise((f.floor(f.col('startYear')/10)*10+9))))))
   df_qury7_2 = df_query7_1.select('tconst','primaryTitle','decade')
   df_query7_3 = df_qury7_2.join(title_ratings, df_qury7_2.tconst == title_ratings.tconst, 'inner')
   window_dept = Window.partitionBy("decade").orderBy(f.col("averageRating").desc())
   df_query7_4 = df_query7_3.withColumn("top", f.row_number().over(window_dept))
   df_query7_5 = df_query7_4.select('primaryTitle','decade','averageRating','numVotes','top').filter(f.col('top') <= 10)
   df_query7_5.show()
   '''







if __name__ == "__main__":
 main()