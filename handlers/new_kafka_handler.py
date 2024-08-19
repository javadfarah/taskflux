import json
import sys
import uuid
import time
import threading
from abc import ABC, abstractmethod
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from handlers.interface import HandlerInterface
from config import env_config


class FactorySchema(ABC):
    @abstractmethod
    def create_producer(self, *args, **kwargs):
        ...

    @abstractmethod
    def create_consumer(self, *args, **kwargs):
        ...

    @abstractmethod
    def _get_config(self, *args, **kwargs):
        ...


class KafkaFactory(FactorySchema):
    """
    Factory class to create Kafka Producers and Consumers.

    Methods:
        create_producer(config): Creates a Kafka producer with the given configuration.
        create_consumer(config, group_id): Creates a Kafka consumer with the given configuration and group ID.
    """

    def __init__(self):
        self.producer = self.create_producer()

    def create_producer(self):
        """
        Creates a Kafka producer instance.

        Args:
            config (dict): Configuration for the Kafka producer.

        Returns:
            Producer: A Kafka producer instance.
        """
        return KafkaProducerManager(config=self._get_config())

    def create_consumer(self, config, group_id):
        conf = config.copy()
        conf.update({
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        return KafkaWorker(config=self._get_config())

    @staticmethod
    def _get_config(broker_url: env_config.BROKER_URL):
        config = {'bootstrap.servers': broker_url}
        return config


class KafkaTopicManager:
    """
    Responsible for managing Kafka topics.

    Methods:
        create_topic_if_not_exists(topic_name, cleanup_policy): Creates a Kafka topic if it doesn't already exist.
    """

    def __init__(self, config):
        """
        Initializes the KafkaTopicManager with the given configuration.

        Args:
            config (dict): Configuration for the Kafka admin client.
        """
        self.admin_client = AdminClient(config)

    def create_topic_if_not_exists(self, topic_name, cleanup_policy="compact"):
        """
        Creates a Kafka topic if it doesn't already exist.

        Args:
            topic_name (str): The name of the topic to create.
            cleanup_policy (str): The cleanup policy for the topic. Defaults to "compact".
        """
        existing_topics = self.admin_client.list_topics(timeout=10).topics
        topic_config = {
            "cleanup.policy": cleanup_policy,
            "compression.type": "producer",
            "retention.bytes": "-1",
        }
        print(topic_config)
        if topic_name not in existing_topics:
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=3, config=topic_config)
            self._create_topic(new_topic)

    def _create_topic(self, new_topic):
        """
        Creates a Kafka topic.

        Args:
            new_topic (NewTopic): The topic to create.
        """
        fs = self.admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic '{topic}' created successfully.")
            except KafkaException as e:
                print(f"Failed to create topic '{topic}': {e}")


class KafkaConsumerManager:
    def __init__(self, topic_name, config):
        """
        Initializes the KafkaProducerManager with a producer and topic name.

        Args:
            producer (Producer): The Kafka producer instance.
            topic_name (str): The name of the Kafka topic."""

        producer = KafkaFactory.create_producer(config)
        self.producer = producer
        self.topic_name = topic_name


class KafkaProducerManager:
    """
    Responsible for managing Kafka producer tasks.

    Methods:
        send(data, key): Sends a message to the Kafka topic.
        update_message(message, data, key): Updates an existing message with new data and sends it.
    """

    def __init__(self, topic_name, config):
        """
        Initializes the KafkaProducerManager with a producer and topic name.

        Args:
            producer (Producer): The Kafka producer instance.
            topic_name (str): The name of the Kafka topic.
        """
        topic_manager = KafkaTopicManager(config)
        topic_manager.create_topic_if_not_exists(topic_name)
        topic_manager.create_topic_if_not_exists(heartbeat_topic_name)

        producer = KafkaFactory.create_producer(config)
        self.producer = producer
        self.topic_name = topic_name

    def send(self, data: bytes, key="default"):
        """
        Sends a message to the Kafka topic.

        Args:
            data (str): The message to send.
            key (str): The key associated with the message. Defaults to "default".
        """
        print(data)
        print(self.topic_name)
        print(key)
        self.producer.produce(topic=self.topic_name, value=data, key=str(key))
        print("produces")
        self.producer.flush()

    def update_message(self, message: dict, data: dict, key: str):
        """
        Updates an existing message with new data and sends it.

        Args:
            message (dict): The original message.
            data (dict): The data to update the message with.
            key (str): The key associated with the message.
        """
        updated_message = {**message, **data}
        serialized_message = json.dumps(updated_message, indent=2).encode('utf-8')
        self.send(data=serialized_message, key=key)


class KafkaHeartbeatManager:
    """
    Manages heartbeat signals for workers.

    Methods:
        _send_heartbeat(): Sends a heartbeat signal.
        _heartbeat_loop(): Continuously sends heartbeat signals at regular intervals.
        _process_heartbeats(consumer): Processes incoming heartbeat signals from other workers.
        _status_check_loop(): Continuously checks the status of workers.
        _check_worker_status(): Checks if any workers have become inactive.
        _start_threads(): Starts the heartbeat and status check threads.
    """

    def __init__(self, producer_manager: KafkaProducerManager, heartbeat_consumer: Consumer, worker_id: str,
                 heartbeat_interval=10,
                 status_check_interval=20):
        """
        Initializes the KafkaHeartbeatManager with a producer manager and interval settings.

        Args:
            producer_manager (KafkaProducerManager): The manager for handling Kafka producer tasks.
            heartbeat_interval (int): Interval (in seconds) between heartbeat signals. Defaults to 60 seconds.
            status_check_interval (int): Interval (in seconds) between worker status checks. Defaults to 120 seconds.
        """
        self.producer_manager = producer_manager
        self.heartbeat_consumer = heartbeat_consumer
        self.heartbeat_interval = heartbeat_interval
        self.status_check_interval = status_check_interval
        self.worker_heartbeats = {}
        self.worker_id = worker_id
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=False)
        self.status_check_thread = threading.Thread(target=self._status_check_loop, daemon=False)

    def _send_heartbeat(self):
        """
        Sends a heartbeat signal indicating that the worker is alive.
        """
        heartbeat_message = json.dumps({"worker_id": self.worker_id, "status": "alive"}).encode(
            'utf-8')
        print("message", heartbeat_message)
        self.producer_manager.send(data=heartbeat_message, key=self.worker_id)

    def _heartbeat_loop(self):
        """
        Continuously sends heartbeat signals at regular intervals.
        """
        while True:
            try:
                self._send_heartbeat()
            except Exception as e:
                print(f"Heartbeat error: {e}")
            time.sleep(self.heartbeat_interval)

    def _process_heartbeats(self, consumer):
        """
        Processes incoming heartbeat signals from other workers.

        Args:
            consumer (Consumer): The Kafka consumer instance used to receive heartbeats.
        """
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                print("nothing")
                break
            print(">>>>>>>", msg.value(), msg.offset(), msg.timestamp()[1])
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write(f'%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n')
                else:
                    raise KafkaException(msg.error())
            else:
                worker_id = json.loads(msg.value()).get("worker_id")
                if worker_id:
                    self.worker_heartbeats[worker_id] = msg.timestamp()[1] / 1e3

    def _status_check_loop(self):
        """
        Continuously checks the status of workers by processing heartbeats and identifying inactive workers.
        """

        self.heartbeat_consumer.subscribe([self.producer_manager.topic_name])

        while True:
            self._process_heartbeats(self.heartbeat_consumer)
            self._check_worker_status()
            time.sleep(self.status_check_interval)

    def _check_worker_status(self):
        """
        Checks if any workers have become inactive based on the time of their last heartbeat.
        """
        current_time = time.time()
        inactive_workers = [worker_id for worker_id, last_heartbeat in self.worker_heartbeats.items()
                            if current_time - last_heartbeat > self.heartbeat_interval * 2]
        print(self.worker_heartbeats.items())
        for worker_id in inactive_workers:
            print(f"Worker {worker_id} is inactive.")


class KafkaWorker:
    def __init__(self, worker_id: str, config):
        self.heartbeat_manager = KafkaHeartbeatManager(producer_manager=heartbeat_producer_manager,
                                                       worker_id=worker_id,
                                                       heartbeat_consumer=self.create_kafka_consumer())
        self._start_heartbeat()
        self.producer_manager = producer_manager
        self.worker_id = str(uuid.uuid4())
        self.consumer = self.create_kafka_consumer()
        self.config = config

    def create_kafka_consumer(self):
        conf = self.config.copy()
        conf.update({
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        return Consumer(conf)

    def change_message_status(self, message: dict, status: str, key: str):

        self.producer_manager.update_message(message, {'status': status}, key)

    def prepare_run_task(self, message: dict, key: str):

        self.producer_manager.update_message(message, {'worker_id': self.worker_id, 'status': 'started'}, key)

    def start(self):
        config = self._kafka_config()
        topic_name = 'task_flux_compacted'
        heartbeat_topic_name = 'task_flux_heartbeat'

        producer_manager = KafkaProducerManager(producer, topic_name)
        heartbeat_producer_manager = KafkaProducerManager(producer, heartbeat_topic_name)

        worker_consumer = KafkaFactory.create_consumer(config, 'task_flux_worker')
        heartbeat_consumer = KafkaFactory.create_consumer(config, 'task_flux_heartbeat')

    def _start_heartbeat(self):
        if not self.heartbeat_manager.heartbeat_thread.is_alive():
            self.heartbeat_manager.heartbeat_thread.start()

    def process_task(self):
        """
        Processes a Kafka task by polling the consumer and executing the task.
        """
        self.consumer.subscribe([self.producer_manager.topic_name])
        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                self._handle_error(msg)
            else:
                message = json.loads(msg.value())
                self.prepare_run_task(message, msg.key())
                # Implement task running logic here
                break

        self.consumer.close()

    def _handle_error(self, msg):
        """
        Handles errors during Kafka message processing.

        Args:
            msg (Message): The Kafka message that encountered an error.
        """
        if msg.error().code() == KafkaError._PARTITION_EOF:
            sys.stderr.write(f'%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n')
        else:
            raise KafkaException(msg.error())


if __name__ == "__main__":
    from config import env_config
