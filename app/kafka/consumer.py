from kafka import KafkaConsumer
from json import loads


class Consumer:
    def __init__(self, topic, group_id, url="localhost:9092"):
        try:
            self.consumer = KafkaConsumer(topic,
                                          group_id=group_id,
                                          bootstrap_servers=url,
                                          auto_offset_reset='earliest',
                                          enable_auto_commit=True,
                                          value_deserializer=lambda x: loads(x.decode('utf-8')))
            print("[INFO] - consumer started")
        except Exception as excp:
            print(f"[ERROR] - could not connect to kafka: {str(excp)}")

    def read_batch(self):
        for message in self.consumer:
            print(f"[INFO] - message read: {message}")