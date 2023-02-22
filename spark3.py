from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from time import sleep


# client = MongoClient('localhost:27017')
# collection = client.weather.weather
# print('mongo client connected')
# sleep(1)

schema = StructType([
    StructField("ozone", IntegerType(), True),
    StructField("particullate_matter", IntegerType(), True),
    StructField("carbon_monoxide", IntegerType(), True),
    StructField("sulfure_dioxide", IntegerType(), True),
    StructField("nitrogen_dioxide", IntegerType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

spark = SparkSession.builder \
    .appName("KafkaStreamData") \
    .getOrCreate()

kafka_topic = "weather"
kafka_server = "localhost:9092"


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", kafka_topic) \
    .option("failOnDataLoss", "false")\
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.ozone", "data.particullate_matter", "data.carbon_monoxide", "data.sulfure_dioxide", "data.nitrogen_dioxide", "data.longitude", "data.latitude", "data.timestamp")



def process_row(row):
    client = MongoClient('localhost:27017')
    data = json.loads(row.toJSON())
    client.weather.weather.insert_one(data)
    print('{} added'.format( data))
    client.close()

df.writeStream.foreach(process_row).start().awaitTermination()