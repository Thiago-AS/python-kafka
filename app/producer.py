from kafka import KafkaProducer
from kafka.errors import KafkaError
from json import dumps


class Producer:
    def __init__(self, url="localhost:9092"):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=url,
                value_serializer=lambda x:
                dumps(x).encode('utf-8'))
        except Exception as excp:
            print(f"[ERROR] - could not connect to kafka: {str(excp)}")

    def success_call_back(self, record):
        print(
            f"[INFO] - message sent: {record.topic} - {record.partition} - {record.offset}")

    def error_call_back(self, exception):
        print(f"[ERROR] - could not send message: {exception}")

    def send(self, topic, msg):
        self.producer.send(
            topic, value=msg).add_callback(self.success_call_back).add_errback(self.error_call_back)
        self.producer.flush()

    def close(self):
        self.producer.close()
        print("[INFO] - producer closed")
