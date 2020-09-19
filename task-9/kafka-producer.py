#kafka-python package pre-installed
from time import sleep
from json import dumps
from kafka import KafkaProducer
import requests

#Kafa server running at sandbox-hdp:6667
producer = KafkaProducer(bootstrap_servers=['sandbox-hdp:6667'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

#request data from api

# using artist id of Michael Jackson: 3fMbdgg4jU18AjLCKBhRSm

url = "https://api.spotify.com/v1/artists/3fMbdgg4jU18AjLCKBhRSm/albums"

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": "Bearer BQBknTnpq4AI34S0EZ-B3_ABwt0Dbc_UuKMKSMsIqx86pCi2Vb4bi3zv0FrxthQrZyy51-wXcHeK8pLW8NTfKlHxYGBdmKmx_RroLvvgxd3LEKmVjzlU5mFeP6Rp_Qkcf76RyvpTZ9NFl4xpNy2JcMDnU7nZyEgz5UNpOBB4QTAZWhvKgLOtp_N3wtri2SIzblC_3xNsXo1TRA2nSKo3nhtatOB5ajys5AZVyCAw-GkHkA-7IxSLOq5f0npY0V0VCo33tL2j7NobA6o-dQ"
    }

response = requests.request("GET", url, headers=headers)

print(response.text)

#send messages into the topic 'bdworld'
producer.send('bdworld', value=response.text)
sleep(2)
