import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
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
                                      t.StructField("primaryName", t.StringType(), False),
                                      t.StructField("birthYear", t.IntegerType(), False),
                                      t.StructField("deathYear", t.IntegerType(), False),
                                      t.StructField("primaryProfession", t.StringType(), False),
                                      t.StructField("knownForTitles", t.StringType(), False)])
   f_name = path_dir_in + '/' + 'name.basics.tsv.gz'
   name_basics = spark_session.read.csv(f_name, header=True, nullValue='null', schema=schema_name_basics, sep='\t')

   schema_title_akas = t.StructType([t.StructField("titleId", t.StringType(), False),
                                     t.StructField("ordering", t.IntegerType(), False),
                                     t.StructField("title", t.StringType(), False),
                                     t.StructField("region", t.StringType(), False),
                                     t.StructField("language", t.StringType(), False),
                                     t.StructField("types", t.StringType(), False),
                                     t.StructField("attributes", t.StringType(), False),
                                     t.StructField("isOriginalTitle", t.IntegerType(), False)])
   f_name = path_dir_in + '/' + 'title.akas.tsv.gz'
   title_akas = spark_session.read.csv(f_name, header=True, nullValue='null', schema=schema_title_akas, sep='\t')

   schema_title_basics = t.StructType([t.StructField("tconst", t.StringType(), False),
                                       t.StructField("titleType", t.StringType(), False),
                                       t.StructField("primaryTitle", t.StringType(), False),
                                       t.StructField("originalTitle", t.StringType(), False),
                                       t.StructField("isAdult", t.IntegerType(), False),
                                       t.StructField("startYear", t.IntegerType(), False),
                                       t.StructField("endYear", t.IntegerType(), False),
                                       t.StructField("runtimeMinutes", t.IntegerType(), False),
                                       t.StructField("genres", t.StringType(), False)])
   f_name = path_dir_in + '/' + 'title.basics.tsv.gz'
   title_basics = spark_session.read.csv(f_name, header=True, nullValue='null', schema=schema_title_basics, sep='\t')

   schema_title_principals = t.StructType([t.StructField("tconst", t.StringType(), False),
                                           t.StructField("ordering", t.IntegerType(), False),
                                           t.StructField("nconst", t.StringType(), False),
                                           t.StructField("category", t.StringType(), False),
                                           t.StructField("job", t.StringType(), False),
                                           t.StructField("characters", t.StringType(), False)])
   f_name = path_dir_in + '/' + 'title.principals.tsv.gz'
   title_principals = spark_session.read.csv(f_name, header=True, nullValue='null', schema=schema_title_principals, sep='\t')

   schema_title_episode = t.StructType([t.StructField("tconst", t.StringType(), False),
                                        t.StructField("parentTconst", t.StringType(), False),
                                        t.StructField("seasonNumber", t.IntegerType(), False),
                                        t.StructField("episodeNumber", t.IntegerType(), False)])
   f_name = path_dir_in + '/' + 'title.episode.tsv.gz'
   title_episode = spark_session.read.csv(f_name, header=True, nullValue='null', schema=schema_title_episode,
                                             sep='\t')

   schema_title_ratings = t.StructType([t.StructField("tconst", t.StringType(), False),
                                                 t.StructField("averageRating", t.DoubleType(), False),
                                                 t.StructField("numVotes", t.IntegerType(), False)])
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

   df_query1 = title_akas.select("title").where(f.col("region") == "UA")
   df_query2 = name_basics.select("primaryName", "birthYear").where((f.col("birthYear") >= 1800) & (f.col("birthYear") < 1900))
   df_query3 = title_basics.select("primaryTitle").where((f.col("titleType") == 'movie') & (f.col("runtimeMinutes") > 120))
   df_query4_temp = title_principals.drop("ordering","job")
   df_query4_temp.show()



if __name__ == "__main__":
 main()