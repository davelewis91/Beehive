#!/usr/bin/python
# -*- coding: utf-8 -*-
import platform
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sqlContext = spark._wrapped
from socket import gethostname
print("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version %s
      /_/
""" % sc.version)
print("Using Python version %s (%s, %s)" % (platform.python_version(),platform.python_build()[0],platform.python_build()[1]))
print("SparkSession available as 'spark', SparkContext available as 'sc', %s available as 'sqlContext'." % sqlContext.__class__.__name__)
print("Running from server %s" % gethostname())
print("Spark Jobs submitted on queue: %s" % sc._conf.get('spark.yarn.queue'))
sc.addPyFile('code/summary.py')
sc.addPyFile('code/handlers.py')

#import sys
#sys.path.append('../code/')
from summary import CollectorBee

if __name__ == '__main__':
    
    print("Starting process")
    db = 'tesco_uk_analyst'
    cb = CollectorBee(spark, db)
    print(cb)
    print(cb._hdfs)
    print(cb.homedir)
    print(cb.my_tables())
    print(cb.list_tables())
    cb.chdir(cb.homedir)
    print(cb.ls())
    cb.delete('test_dir')
    cb.mkdir('test_dir')
    cb.delete('test_dir')
    cb.chdir('/tesco_uk/data/unrestricted/analyst/zoim/')
    print(cb.file_size('zoim_training'))
    spark.stop()
    sc.stop()
    print("End of the job. Bye!")

    