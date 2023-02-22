from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import csv
from time import sleep



# Define the schema of the incoming Kafka stream
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

# Create a SparkSession object
spark = SparkSession.builder \
    .appName("KafkaStreamData") \
    .getOrCreate()

# Define the Kafka topic and server details
kafka_topic = "weather"
kafka_server = "localhost:9092"

# Read the Kafka stream into a DataFrame
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
# Print the received stream data to console
f = open('./op.csv', 'w')
writer = csv.writer(f)

def process_row(row):
    f = open('./op.csv', 'a')
    writer = csv.writer(f)
    writer.writerow(row)
    f.close()


query = df \
    .writeStream.foreach(process_row).start().awaitTermination()
    # .outputMode("append") \
    # .format("csv") \
    # .option("path", "output") \
    # .option("checkpointLocation", "checkpoint") \
    # .start()
