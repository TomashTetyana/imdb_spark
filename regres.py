import pyspark.sql.functions as f
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from settings import *
import read_write
def regres(spark_session):
    '''
    building a regression to predict the rating by genre, duration, region

    :param spark_session: spark session id
    :return:
    '''
    f_1 = path_dir_in + '/' + 'title.akas.tsv.gz'
    f_2 = path_dir_in + '/' + 'title.basics.tsv.gz'
    f_3 = path_dir_in + '/' + 'title.ratings.tsv.gz'
    title_akas = read_write.read_imdb(spark_session, f_1, schema_title_akas)
    title_basics = read_write.read_imdb(spark_session, f_2, schema_title_basics)
    title_ratings = read_write.read_imdb(spark_session, f_3, schema_title_ratings)
    #  selection data from dataframe
    df_query1 = title_akas.join(title_basics, title_akas.titleId == title_basics.tconst, 'inner')
    df_query2 = df_query1.join(title_ratings, df_query1.tconst == title_ratings.tconst, 'inner')
    df_query3 = df_query2.select('titleId','region','isAdult','runtimeMinutes','genres','averageRating')

    # delete null value from dataframe
    df_query4 = df_query3.filter(f.col('region') != '\\N')
    df_query5 = df_query4.dropDuplicates(['titleId'])
    df_query6 = df_query5.na.fill(value=1,subset=['runtimeMinutes'])

    # indexing of text data
    indexer = StringIndexer(inputCol="region", outputCol="region_id")
    data = indexer.fit(df_query6).transform(df_query6)
    indexer1 = StringIndexer(inputCol="genres", outputCol="genres_id")
    data1 = indexer1.fit(data).transform(data)

    # Vector assembler to combine raw elements  into a single element vector
    vectorAssembler = VectorAssembler(
        inputCols=['region_id', 'isAdult', 'runtimeMinutes', 'genres_id'],
        outputCol='features')
    data_df = vectorAssembler.transform(data1)
    data_df = data_df.select(['features', 'averageRating'])
    lr = LinearRegression(featuresCol='features', labelCol='averageRating', maxIter=10, regParam=0.3,
                                                 elasticNetParam=0.8)
    lr_model = lr.fit(data_df)
    file = open('../result/regres.txt', 'w')
    file.write("Coefficients: " + str(lr_model.coefficients))
    file.write("Intercept: " + str(lr_model.intercept))
    trainingSummary = lr_model.summary
    file.write("RMSE: %f" % trainingSummary.rootMeanSquaredError)
    file.write("r2: %f" % trainingSummary.r2)
