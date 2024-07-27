# producer
from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(20):
    msg = f"message {i} : Xin Chao, Toi la HaUIer"
    producer.send("sic-kafka", bytes(msg, 'utf-8'))
    time.sleep(10)
