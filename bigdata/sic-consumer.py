from kafka import KafkaConsumer, KafkaProducer
import pickle
import json
import ast

consumer = KafkaConsumer(bootstrap_servers="localhost:9092", auto_offset_reset='latest')
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
consumer.subscribe(['sic-broker-input'])


# Load
with open('svm_model.pkl', 'rb') as f:
    svm_clf = pickle.load(f)

# stream consumer
for msg in consumer:
    msg_data = msg.value.decode("utf-8") # convert bytes to str

    json_data = json.loads(msg_data) # convert str to json(dict)

    print("Json >>> ", json_data, type(json_data))

    y_pred = svm_clf.predict([json_data['record']])

    print("y_pred >>>> ", y_pred)

    msg_output = {
        "uuid": json_data['uuid'],
        "predict": str(y_pred[0])
    }
    
    print("output >>> ", msg_output)

    producer.send("sic-broker-output", msg_output)

    print("Push data to topic sic-broker-output Done!")