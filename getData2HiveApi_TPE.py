##getData2HiveApi_TPE.py
import requests
from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, HiveContext,SQLContext
from pyspark.sql import functions as F
import json
import time


if __name__ == '__main__':
    today = time.strftime("%Y%m%d", time.localtime())

    params = {'access_key': '6d9483427721b5e480d5e20e17ab05dc'}
    api_result = requests.get(
        'http://api.aviationstack.com/v1/flights?access_key=6d9483427721b5e480d5e20e17ab05dc&flight_status=active&limit=100&dep_iata=TPE',
        params)
    api_response = api_result.text

    spark = (SparkSession
             .builder
             .appName('example-pyspark-read-and-write-from-hive')
             .config("hive.metastore.uris", "thrift://192.168.42.129:9083", conf=SparkConf())
             .enableHiveSupport()
             .getOrCreate()
             )
    sc = spark.sparkContext

    df = spark.read.json(sc.parallelize([api_response]))

    df = df.select(F.explode('data')).withColumnRenamed("col", "data") \
        .selectExpr('data', 'data.flight_date') \
        .withColumn('flight_dt', F.regexp_replace('flight_date', '-', '').astype('Int')).drop('flight_date') \
        .select('flight_dt', 'data')

    df.createOrReplaceTempView('TmpData')

    inputdf = spark.sql('select * from TmpData')
    spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
    spark.sql("set hive.exec.dynamic.partition=true")
    write_sql = 'insert overwrite table ods_flight.flight_api_data partition (flight_dt) select data,flight_dt from TmpData'

    spark.sql(write_sql)

