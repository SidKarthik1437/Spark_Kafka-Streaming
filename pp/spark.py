from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_csv
from pyspark.sql.types import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import RandomForestClassifier
import csv

spark = SparkSession.builder.appName("KafkaSparkIntegration").getOrCreate()

schema = StructType([
    StructField("DO", FloatType()),
    StructField("pH", FloatType()),
    StructField("ORP", FloatType()),
    StructField("Cond", FloatType()),
    StructField("Temp", FloatType()),
    StructField("WQI", FloatType()),
    StructField("Status",StringType())
])

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer

 
dataStream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather") \
    .option("failOnDataLoss", 'False')\
    .load()\
    .selectExpr("CAST(value AS STRING)")\
    .select(from_json(col("value"), schema).alias('data'))\
    .select('data.DO','data.pH','data.ORP','data.Cond','data.Temp','data.WQI','data.Status')

data = spark.read.csv("./sangam.csv",header=True,inferSchema='True')
print('import', type(data))

ind = StringIndexer(inputCol = 'Status', outputCol = 'Status_index')
data=ind.fit(data).transform(data)
print('indexed',data)

numericCols = ['DO', 'pH', 'ORP', 'Cond','Temp','WQI']
assembler = VectorAssembler(inputCols=numericCols, outputCol="features")
data = assembler.transform(data)

res = data.select('features','Status_index')
print('assembled',res)


rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'Status_index')
rfModel = rf.fit(res)
#rfModel = rf.load(path='./rf_model').overwrite().save(path='./rf_model')
print('model training completed')


# dataStream = dataStream.select(from_json(col=("Status"), schema=schema.alias('data')))
# ind = StringIndexer(inputCol = 'Status', outputCol = 'Status_index')
# ind = ind.fit(dataStream).transform(dataStream)



def status(output):
    if output == 0.0:
        return 'Very Poor'
    elif output == 1.0:
        return 'Poor'
    elif output == 2.0:
        return 'Fair'
    elif output == 3.0:
        return 'Good'
    elif output == 4.0:
        return 'Excellent'

def process_row(row):
    row = status(row)
    f = open('./op.csv', 'a')
    writer = csv.writer(f)
    writer.writerow(row)    
    f.close()

numericCols = ['DO', 'pH', 'ORP', 'Cond', 'Temp', 'WQI']
assembler = VectorAssembler(inputCols=numericCols, outputCol="features")
dataStream = assembler.transform(dataStream)

predictions = rfModel.transform(dataStream.select('features'))
#predictions = predictions.withColumn('Status', status(predictions.prediction))
output = predictions\
.selectExpr("CAST(prediction AS STRING)")


# .selectExpr("to_json(struct(*)) AS value")
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# evaluator = MulticlassClassificationEvaluator(labelCol="Status_index", predictionCol="prediction")
# accuracy = evaluator.evaluate(lrPrediction)
# print("Accuracy = %s" % (accuracy))
# print("Test Error = %s" % (1.0 - accuracy))

query = output.writeStream.foreach(process_row).start()
query.awaitTermination()



# query = lrModel.transform(dataStream).writeStream.foreach(process_row).start()

# # outputStream = lrModel.transform(testingData) \
# #   .selectExpr("CAST(label AS STRING) AS key", "to_json(struct(*)) AS value") \
# #   .writeStream \
# #   .format("kafka") \
# #   .option("kafka.bootstrap.servers", "localhost:9092") \
# #   .option("topic", "output-topic") \
# #   .option("checkpointLocation", "checkpoint") \
# #   .start()

