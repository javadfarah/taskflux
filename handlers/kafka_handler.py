import json
import sys
import uuid

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from handlers.interface import HandlerInterface


class KafkaHandler(HandlerInterface):
    def __init__(self, config, topic_name):
        self.config = config
        self.topic_name = f"task_flux_{topic_name}"
        self._create_topic_if_not_exists()

    def _create_topic_if_not_exists(self):
        admin_client = AdminClient(self.config)
        topic_list = admin_client.list_topics(timeout=10).topics

        if self.topic_name not in topic_list:
            topic_config = {
                "cleanup.policy": "compact",
                "compression.type": "producer",
                "retention.bytes": "-1",
            }
            new_topic = NewTopic(self.topic_name, num_partitions=1, replication_factor=3, config=topic_config)
            admin_client.create_topics([new_topic])
            print(f"Topic '{self.topic_name}' created successfully.")

    def send(self, task_data, key="default"):
        producer = self._get_producer_instance()
        producer.produce(topic=self.topic_name, value=task_data, key=str(key))
        producer.flush()

    def get_latest(self, key):
        consumer_conf = self.config.copy()
        consumer_conf.update({
            'group.id': uuid.uuid4(),
            'auto.offset.reset': 'earliest'
        })
        consumer = Consumer(consumer_conf)
        consumer.subscribe([self.topic_name])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                print("None")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                return json.loads(msg.value())

        consumer.close()

    def _get_config(self):
        return self.config

    def _get_producer_instance(self):
        conf = self._get_config()
        return Producer(conf)

    def get(self, *args, **kwargs):
        pass

    @staticmethod
    def handler_inputs(broker_url: str, queue_name: str):
        conf = {
            'bootstrap.servers': broker_url,
        }
        inputs = dict(config=conf, topic_name=queue_name)
        return inputs
