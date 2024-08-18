import json
import sys
import uuid
import time
import threading
from abc import ABC, abstractmethod
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from handlers.interface import HandlerInterface


class KafkaFactory:
    """
    Factory class to create Kafka Producers and Consumers.

    Methods:
        create_producer(config): Creates a Kafka producer with the given configuration.
        create_consumer(config, group_id): Creates a Kafka consumer with the given configuration and group ID.
    """

    @staticmethod
    def create_producer(config: dict):
        """
        Creates a Kafka producer instance.

        Args:
            config (dict): Configuration for the Kafka producer.

        Returns:
            Producer: A Kafka producer instance.
        """
        return Producer(config)

    @staticmethod
    def create_consumer(config, group_id):
        """
        Creates a Kafka consumer instance.

        Args:
            config (dict): Configuration for the Kafka consumer.
            group_id (str): The group ID for the Kafka consumer.

        Returns:
            Consumer: A Kafka consumer instance.
        """
        conf = config.copy()
        conf.update({
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        return Consumer(conf)


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


class KafkaProducerManager:
    """
    Responsible for managing Kafka producer tasks.

    Methods:
        send(data, key): Sends a message to the Kafka topic.
        update_message(message, data, key): Updates an existing message with new data and sends it.
    """

    def __init__(self, producer, topic_name):
        """
        Initializes the KafkaProducerManager with a producer and topic name.

        Args:
            producer (Producer): The Kafka producer instance.
            topic_name (str): The name of the Kafka topic.
        """
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


class TaskHandler(ABC):
    """
    Abstract class for task handling.

    Methods:
        process_task(): Processes a task.
        change_message_status(message, status, key): Changes the status of a message.
        prepare_run_task(message, key): Prepares a message for task execution.
    """

    def __init__(self, producer_manager):
        """
        Initializes the TaskHandler with a producer manager.

        Args:
            producer_manager (KafkaProducerManager): The manager for handling Kafka producer tasks.
        """
        self.producer_manager = producer_manager
        self.worker_id = str(uuid.uuid4())

    @abstractmethod
    def process_task(self, message: dict, key: str):
        """
        Abstract method for processing a task.

        Args:
            message (dict): The message to process.
            key (str): The key associated with the message.
        """
        pass

    def change_message_status(self, message: dict, status: str, key: str):
        """
        Changes the status of a message.

        Args:
            message (dict): The message to update.
            status (str): The new status to set.
            key (str): The key associated with the message.
        """
        self.producer_manager.update_message(message, {'status': status}, key)

    def prepare_run_task(self, message: dict, key: str):
        """
        Prepares a message for task execution.

        Args:
            message (dict): The message to prepare.
            key (str): The key associated with the message.
        """
        self.producer_manager.update_message(message, {'worker_id': self.worker_id, 'status': 'started'}, key)


class KafkaTaskHandler(TaskHandler):
    """
    Concrete class that handles Kafka task processing.

    Methods:
        process_task(): Processes a Kafka task.
    """

    def __init__(self, producer_manager: KafkaProducerManager, consumer: Consumer):
        """
        Initializes the KafkaTaskHandler with a producer manager and consumer.

        Args:
            producer_manager (KafkaProducerManager): The manager for handling Kafka producer tasks.
            consumer (Consumer): The Kafka consumer instance.
        """
        super().__init__(producer_manager)
        self.consumer = consumer

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
        self._start_threads()

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
                    self.worker_heartbeats[worker_id] = msg.timestamp()[1]/1e3

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

    def _start_threads(self):
        """
        Starts the heartbeat and status check threads.
        """
        print("start threads")
        if not self.heartbeat_thread.is_alive():
            self.heartbeat_thread.start()
        if not self.status_check_thread.is_alive():
            self.status_check_thread.start()


if __name__ == "__main__":
    from config import env_config

    config = {'bootstrap.servers': env_config.BROKER_URL}
    topic_name = 'task_flux_compacted'
    heartbeat_topic_name = 'task_flux_heartbeat'

    topic_manager = KafkaTopicManager(config)
    topic_manager.create_topic_if_not_exists(topic_name)
    topic_manager.create_topic_if_not_exists(heartbeat_topic_name)

    producer = KafkaFactory.create_producer(config)
    producer_manager = KafkaProducerManager(producer, topic_name)
    heartbeat_producer_manager = KafkaProducerManager(producer, heartbeat_topic_name)

    worker_consumer = KafkaFactory.create_consumer(config, 'task_flux_worker')
    heartbeat_consumer = KafkaFactory.create_consumer(config, 'task_flux_heartbeat')
    task_handler = KafkaTaskHandler(producer_manager=producer_manager, consumer=worker_consumer)
    # task_handler.process_task()
    print("abbas")
    heartbeat_manager = KafkaHeartbeatManager(producer_manager=heartbeat_producer_manager,
                                              worker_id=task_handler.worker_id, heartbeat_consumer=heartbeat_consumer)
    print("heeeeeyyyyyy")
