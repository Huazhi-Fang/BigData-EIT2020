#kafka-python package pre-installed
from time import sleep
from json import dumps
from kafka import KafkaProducer
import requests

#Kafa server running at sandbox-hdp:6667
producer = KafkaProducer(bootstrap_servers=['sandbox-hdp:6667'],
                        value_serializer=lambda x: dumps(x).encode('utf-8'))

#request data from api
url = "https://amazon-deals.p.rapidapi.com/amazon-offers/all"

headers = {
    'x-rapidapi-host': "amazon-deals.p.rapidapi.com",
    'x-rapidapi-key': "<x-rapidapi-key>"
    }

response = requests.request("GET", url, headers=headers)

#send messages into the topic 'bdworld'
producer.send('bdworld', value=response.text)
sleep(2)
