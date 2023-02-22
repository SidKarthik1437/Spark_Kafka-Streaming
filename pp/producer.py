# from kafka import KafkaProducer
# import csv

# # Create a Kafka producer
# producer = KafkaProducer(bootstrap_servers='localhost:9092')

# # Open the CSV file for reading
# with open('sangam.csv', 'r') as csvfile:
#     # Create a CSV reader object
#     reader = csv.reader(csvfile)

#     # Iterate over each row in the CSV file
#     for row in reader:
#         # Convert the row to a string and send it to the Kafka topic
#         producer.send('sangam', value=bytes(','.join(row), 'utf-8'))
#         print(row)   

# # Wait for any outstanding messages to be sent and delivery reports to be received


from time import sleep
from json import dumps
from kafka import KafkaProducer
import pandas as pd

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

data = pd.read_csv('./sangam.csv')
# iterate over rows in the data frame and send each row to Kafka
for index, row in data.iterrows():
    # convert row to JSON object
    data = row.to_dict()
    
    # send JSON object to Kafka topic
    producer.send('weather', value=data)
    print(data)
    print(type(data))
    sleep(0.1)
