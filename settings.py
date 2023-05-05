'''
file paths, data schemas
'''
import pyspark.sql.types as t
path_dir_in = r'../imdb-spark/data'
path_dir_out=r'../imdb-spark/result'

schema_name_basics = t.StructType([t.StructField('nconst', t.StringType(), False),
                                   t.StructField('primaryName', t.StringType(), True),
                                   t.StructField("birthYear", t.IntegerType(), True),
                                   t.StructField("deathYear", t.IntegerType(), True),
                                   t.StructField("primaryProfession", t.StringType(), True),
                                   t.StructField("knownForTitles", t.StringType(), True)])
schema_title_akas = t.StructType([t.StructField("titleId", t.StringType(), False),
                                     t.StructField("ordering", t.IntegerType(), True),
                                     t.StructField("title", t.StringType(), True),
                                     t.StructField("region", t.StringType(), True),
                                     t.StructField("language", t.StringType(), True),
                                     t.StructField("types", t.StringType(), True),
                                     t.StructField("attributes", t.StringType(), True),
                                     t.StructField("isOriginalTitle", t.IntegerType(), True)])
schema_title_basics = t.StructType([t.StructField("tconst", t.StringType(), False),
                                       t.StructField("titleType", t.StringType(), True),
                                       t.StructField("primaryTitle", t.StringType(), True),
                                       t.StructField("originalTitle", t.StringType(), True),
                                       t.StructField("isAdult", t.IntegerType(), True),
                                       t.StructField("startYear", t.IntegerType(), True),
                                       t.StructField("endYear", t.IntegerType(), True),
                                       t.StructField("runtimeMinutes", t.IntegerType(), True),
                                       t.StructField("genres", t.StringType(), True)])
schema_title_principals = t.StructType([t.StructField("tconst", t.StringType(), False),
                                           t.StructField("ordering", t.IntegerType(), True),
                                           t.StructField("nconst", t.StringType(), True),
                                           t.StructField("category", t.StringType(), True),
                                           t.StructField("job", t.StringType(), True),
                                           t.StructField("characters", t.StringType(), True)])
schema_title_episode = t.StructType([t.StructField("tconst", t.StringType(), False),
                                        t.StructField("parentTconst", t.StringType(), True),
                                        t.StructField("seasonNumber", t.IntegerType(), True),
                                        t.StructField("episodeNumber", t.IntegerType(), True)])
schema_title_ratings = t.StructType([t.StructField("tconst", t.StringType(), False),
                                                 t.StructField("averageRating", t.DoubleType(), True),
                                                 t.StructField("numVotes", t.IntegerType(), True)])
schema_title_crew = t.StructType([t.StructField("tconst", t.StringType(), False),
                                     t.StructField("directors", t.StringType(), True),
                                     t.StructField("writers", t.StringType(), True)])