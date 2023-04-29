import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
conf = SparkConf()
conf.setMaster("local").setAppName('My app')
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
print('Запущен Spark версии', spark.version)
path_dir = r'../imdb-spark/data'
name_bas=path_dir+'/'+'name.basics.tsv.gz'
imdb_movies = spark.read.csv(name_bas,sep = '\t')
print(imdb_movies.describe())
print(imdb_movies.show())
