from time import sleep
from json import dumps
from kafka import KafkaProducer
import pandas as pd

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

data = pd.read_csv('./data/pollutionData158324.csv')
# iterate over rows in the data frame and send each row to Kafka
for index, row in data.iterrows():
    # convert row to JSON object
    data = row.to_dict()
    
    # send JSON object to Kafka topic
    producer.send('weather', value=data)
    print(data)
    print(type(data))
    sleep(1)

# close Kafka producer
