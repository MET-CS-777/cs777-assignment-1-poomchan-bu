from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


# Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

# Function - Cleaning
# For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

# Main
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")

    # Task 1
    # Your code goes here
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
    result = count_by_key.top(10, key=lambda x: x[1])
    
    rdd_result = sc.parallelize(result)
    rdd_result.coalesce(1).saveAsTextFile(sys.argv[2])

    sc.stop()