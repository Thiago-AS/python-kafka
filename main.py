from app.client import KafkaClient
from app.producer import Producer

# ~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test_topic --from-beginning

if __name__ == '__main__':
    kafka_client = KafkaClient('test')
    #kafka_client.create_topics(['test_topic'])

    producer = Producer()
    producer.send('test_topic', "test_message 2")

    producer.close()
    kafka_client.close()

