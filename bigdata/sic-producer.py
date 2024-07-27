# producer
from kafka import KafkaProducer
import time
from tqdm import tqdm
import pandas as pd
import uuid
import json



cols = ['Administrative', 'Administrative_Duration', 'Informational',
       'Informational_Duration', 'ProductRelated', 'ProductRelated_Duration',
       'BounceRates', 'ExitRates', 'PageValues', 'SpecialDay',
       'OperatingSystems', 'Browser', 'Region', 'TrafficType', 'Revenue']

df = pd.read_csv("/home/quang.domanh/kafka_2.13-3.7.1/online_shoppers_intention.csv", usecols=cols)
input_data = df.head(100).values[:, : 14]
print("INPUT : ", input_data.shape)

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for _, record in tqdm(enumerate(input_data)):
    
    msg = {
        "uuid": str(uuid.uuid1()),
        "record": list(record),
    }

    # print(msg)


    producer.send("sic-broker-input", msg)
    time.sleep(20)

