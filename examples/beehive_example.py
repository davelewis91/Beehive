"""
EXAMPLE USAGE OF THE BEEHIVE CLASSES
Gets net spend on certain items in express in one month
"""

#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
import platform
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as F
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
sc.addPyFile('code/handlers.py')
sc.addPyFile('code/worker.py')

from worker import WorkerBee

# create your worker
worker = WorkerBee(spark, 'source_df', 'analyst_db')
worker.chdir('Bank/')  # I'm know I'm already in my home directory
# move csv file to hive
worker.mv_to_hive('examples/prods_of_interest.csv', 
                  'prods_of_interest.csv', 
                  overwrite=True)

# read in transaction data
print('Reading in transactions')
trans_all = worker.read_table('transaction_item_mft')
min_date = '2017-08-01'
max_date = '2017-08-30'
trans = trans_all.select('store_id', 'prod_id', 'date_id', 'net_spend_amt')\
    .filter((F.col('date_id') >= F.to_date(F.lit(min_date)))
            &(F.col('date_id') <= F.to_date(F.lit(max_date))))

# read in store data
print('Reading in stores')
store_all = worker.read_table('store_dim_c')
store = store_all.filter(store_all.format_code == 'E').select('store_id')

# read in prod data
print('Reading in products')
prod_all = worker.read_table('prod_dim_c')
prod = prod_all.select('prod_id', 'prod_group_code').distinct()

# join prod to prods_of_interest
print('Joining prods to prods_of_interest')
prods_of_int = spark.read.csv(worker.currentdir+'prods_of_interest.csv', header=True)
prod = prod.join(F.broadcast(prods_of_int), how='inner', on='prod_group_code')

# join it all together
print('Joining everything together')
tmp = trans.join(F.broadcast(prod), how='inner', on='prod_id')\
    .join(F.broadcast(store), how='inner', on='store_id')

# do the groupby
print('Doing the groupby')
final = tmp.select('prod_group_code', 'net_spend_amt').groupby('prod_group_code')\
    .agg(F.sum('net_spend_amt').alias('net_spend'))

# save it as a table in my personal area
print('Saving table')
worker.save_table(final, 'prod_spend_in_august')

# export table to csv file in /nfs/
print('Exporting table')
worker.export_table('prod_spend_in_august', 'examples/final_result.csv')

print('Dropping table')
worker.drop_table('prod_spend_in_august')
    
spark.stop()
sc.stop()
print("Worker: Job done... (WC3 reference? Anybody?)")