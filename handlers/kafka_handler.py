import json
import sys
import uuid
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from handlers.interface import HandlerInterface
from task import Task


class KafkaHandler(HandlerInterface):

    def __init__(self, config, topic_name):
        self.config = config
        self.queue_name = topic_name
        self.topic_name = "task_flux_compacted"
        self._create_topic_if_not_exists()
        self.worker_id = str(uuid.uuid4())

    def __del__(self):
        self.cleanup()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

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

    def update_message(self, message: dict, data: dict, key: str):
        message = message | data
        message = json.dumps(message, indent=2).encode('utf-8')
        self.send(task_data=message, key=key)

    def change_message_status(self, message: dict, status: str, key: str):
        data = dict(status=status)
        self.update_message(message=message, data=data, key=key)

    def prepare_run_task(self, message: dict, key: str):
        self.update_message(message=message, data={'worker_id': self.worker_id, "status": "started"}, key=key)

    def get_latest(self):
        consumer_conf = self.config.copy()
        consumer_conf.update({
            'group.id': self.queue_name,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False  # Disable auto commit
        })
        consumer = Consumer(consumer_conf)
        consumer.subscribe([self.topic_name])
        consumer_id = consumer.memberid()
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
                message = json.loads(msg.value())
                message_key = msg.key()
                self.prepare_run_task(message=message, key=message_key)
                # Task.run_task(message)
                break

        consumer.close()
        self.cleanup()

    def _get_config(self):
        return self.config

    def _get_producer_instance(self):
        conf = self._get_config()
        return Producer(conf)

    def get(self, *args, **kwargs):
        pass

    def cleanup(self):
        print("cleanup")
        # admin_client = AdminClient(self.config)
        # admin_client.delete_consumer_groups([self.group_id])

    @staticmethod
    def handler_inputs(broker_url: str, queue_name: str = "default"):
        conf = {
            'bootstrap.servers': broker_url,
        }
        inputs = dict(config=conf, topic_name=queue_name)
        return inputs
