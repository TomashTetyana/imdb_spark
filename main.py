import os
from pyspark import SparkConf, SparkContext
#from pyspark.sql import SparkSession
from task_1 import *
from task_2 import *
from task_3 import *
from task_4 import *
from task_5 import *
from task_6 import *
from task_7 import *
from task_8 import *

def main():
    '''
    The main function of the project
    A Spark session is started
    Tasks are performed sequentially.
    The session ID is passed to them as a parameter

    '''
    spark_session = (SparkSession.builder
                    .master("local")
                    .appName("task app")
                    .config(conf=SparkConf())
                    .getOrCreate())

    task_1(spark_session)
    task_2(spark_session)
    task_3(spark_session)
    task_4(spark_session)
    task_5(spark_session)
    task_6(spark_session)
    task_7(spark_session)
    task_8(spark_session)



if __name__ == "__main__":
 main()