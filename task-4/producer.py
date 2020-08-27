#kafka-python package pre-installed
from time import sleep
from json import dumps
from kafka import KafkaProducer

#Kafa server running at localhost:9093
producer = KafkaProducer(bootstrap_servers=['localhost:9093'],
                     	value_serializer=lambda x: dumps(x).encode('utf-8')
)

#send messages into the topic 'bdworld'
with open('./Shakespeare.txt', 'r') as f:
	for line in f.readlines():
    	producer.send('bdworld', value=line)
    	sleep(2)