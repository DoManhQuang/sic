from kafka import KafkaConsumer, TopicPartition
consumer = KafkaConsumer(bootstrap_servers="localhost:9092")
consumer.subscribe('sic-kafka')

# stream consumer
for msg in consumer:
    print(msg.value)