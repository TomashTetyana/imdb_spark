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

   df_query1 = title_akas.select("title", "region").where(f.col("region") == "UA")
   df_query1.show()

   df_query2 = name_basics.select("primaryName", "birthYear").where((f.col("birthYear") >= 1800) & (f.col("birthYear") < 1900))
   df_query2.show()



   #print(title_basics.show())

if __name__ == "__main__":
 main()