from kafka.admin import KafkaAdminClient, NewTopic


class KafkaClient:
    def __init__(self, client, url="localhost:9092"):
        self.client = KafkaAdminClient(
            bootstrap_servers=url, client_id=client)

    def create_topics(self, topics):
        topics_list = [NewTopic(name=topic,
                                num_partitions=1, replication_factor=1) for topic in topics]
        self.client.create_topics(new_topics=topics_list, validate_only=False)

    def close_topics(self, topics):
        self.client.delete_topics(topics)

    def close(self):
        self.client.close()
