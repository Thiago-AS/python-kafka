from app.kafka.client import KafkaClient
from app.kafka.producer import Producer
from app.kafka.consumer import Consumer

# ~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test_topic --from-beginning

if __name__ == '__main__':
    kafka_client = KafkaClient('test')
    producer = Producer()
    consumer = Consumer('test_topic', 'my_group')
    for i in range(1000):
        producer.send('test_topic', {'value': i})

    consumer.read_batch()

    producer.close()
    # kafka_client.close_topics(['test_topic'])
    kafka_client.close()

