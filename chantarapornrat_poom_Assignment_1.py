from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles, 
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    rdd = sc.textFile(sys.argv[1])

    #Task 1
    #Your code goes here
    sqlContext = SQLContext(sc)
    file = sys.argv[1]
    df = sqlContext.read.format('csv').options(header='false', inferSchema='true',  sep =",").load(file)
    rdd = df.rdd.map(tuple)

    # Clean data
    clean_rdd = rdd.filter(correctRows)
    
    # Select only necessary information (taxi-driver pairs)
    taxi_driver = clean_rdd.map(lambda x: (x[0], x[1]))
    
    # Filter only distinct taxi-drivers pairs
    distinct_taxi_driver = taxi_driver.distinct()

    # Count the number of pairs
    count_by_key = distinct_taxi_driver.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

    # Collect top result (sort by value)
    result1 = count_by_key.top(10, key=lambda x: x[1])
    
    rdd_result1 = sc.parallelize(result1)
    rdd_result1.coalesce(1).saveAsTextFile(sys.argv[2])


    #Task 2
    # Your code goes here
    
    # Select only necessary information (driver, time, money)
    task2_data = clean_rdd.map(lambda x: (x[0], x[4], x[16]))
    
    # Calculate money earned per minute
    def calculate_money_per_minute(x):
        time = x[1]/60 # minutes
        money = x[2]
        return (x[0], money/time)
    
    task2_calculated = task2_data.map(calculate_money_per_minute)

    # Collect top result (sort by value)
    result2 = task2_calculated.top(10, key=lambda x: x[1])

    #savings output to argument
    rdd_result2 = sc.parallelize(result2)
    rdd_result2.coalesce(1).saveAsTextFile(sys.argv[3])

    #Task 3 - Optional 
    #Your code goes here

    #Task 4 - Optional 
    #Your code goes here

    sc.stop()