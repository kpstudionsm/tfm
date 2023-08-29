import json
from time import sleep
from json import dumps
from kafka import KafkaProducer

topic_name="StreamingTopic"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,11,5),
                             value_serializer=lambda x:dumps(x).encode('utf-8'))
def sendKafkaStreaming(jsonTweet):
    data= json.loads(jsonTweet)
    text = data['data']['text']
    print("HOALA")
    producer.send(topic_name,value=text)
    sleep(0.5)

def sendKafkaBatch():
    topic_name = "BatchTopic"
    # Batch File
    nameFile = "Files/test_kafka.csv"
    file = open(nameFile, "r", encoding="utf8")
    file.readline()

    for e in range(10):
        data = file.readline()
        print(data)
        producer.send(topic_name, value=data)
        sleep(2)