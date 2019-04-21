from confluent_kafka.admin import NewTopic, AdminClient


class KafkaAdminMgr(object):

    def __init__(self):

        self.KafkaAdmin = AdminClient({'bootstrap.servers': 'localhost:9092'})

    def addTopics(self, topics):

        new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics]
        self.KafkaAdmin.create_topics(new_topics)
