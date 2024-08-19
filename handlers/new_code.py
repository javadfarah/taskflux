import os
import json
import sys
import uuid
import time
import threading
from abc import ABC, abstractmethod
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from concurrent.futures import ThreadPoolExecutor


# Abstract Handler Interface
class MessageBrokerHandler(ABC):
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def process_task(self, msg):
        pass


class KafkaProducerManager:
    def __init__(self, config, topic_name):
        self.producer = Producer(config)
        self.topic_name = topic_name

    def send(self, data: bytes, key="default"):
        self.producer.produce(topic=self.topic_name, value=data, key=str(key))
        self.producer.flush()


class KafkaConsumerManager:
    def __init__(self, config, topic_name):
        conf = config.copy()
        conf.update({
            'group.id': topic_name,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        self.consumer = Consumer(conf)
        self.topic_name = topic_name

    def start(self):
        self.consumer.subscribe([self.topic_name])

    def poll(self, timeout=1.0):
        return self.consumer.poll(timeout=timeout)

    def stop(self):
        self.consumer.close()


class KafkaHeartbeatManager:
    def __init__(self, producer_manager, consumer_manager, heartbeat_interval=10, status_check_interval=20):
        self.producer_manager = producer_manager
        self.consumer_manager = consumer_manager
        self.heartbeat_interval = heartbeat_interval
        self.status_check_interval = status_check_interval
        self.worker_heartbeats = {}

    def _send_heartbeat(self, worker_id):
        heartbeat_message = json.dumps({"worker_id": worker_id, "status": "alive"}).encode('utf-8')
        self.producer_manager.send(data=heartbeat_message, key=worker_id)

    def _heartbeat_loop(self, worker_id):
        while True:
            try:
                self._send_heartbeat(worker_id)
            except Exception as e:
                print(f"Heartbeat error: {e}")
            time.sleep(self.heartbeat_interval)

    def _process_heartbeats(self):
        while True:
            msg = self.consumer_manager.poll(timeout=1.0)
            if msg is None:
                break
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
        self.consumer_manager.start()
        while True:
            self._process_heartbeats()
            self._check_worker_status()
            time.sleep(self.status_check_interval)

    def _check_worker_status(self):
        current_time = time.time()
        inactive_workers = [worker_id for worker_id, last_heartbeat in self.worker_heartbeats.items()
                            if current_time - last_heartbeat > self.heartbeat_interval * 2]
        for worker_id in inactive_workers:
            print(f"Worker {worker_id} is inactive.")

    def start_heartbeat(self, worker_id):
        return threading.Thread(target=self._heartbeat_loop, args=(worker_id,), daemon=False).start()

    def start_heartbeat_check(self):
        return threading.Thread(target=self._status_check_loop, daemon=False).start()


class KafkaHandler(MessageBrokerHandler):
    def __init__(self, producer_manager, consumer_manager, heartbeat_consumer_manager):
        self.producer_manager = producer_manager
        self.consumer_manager = consumer_manager
        self.heartbeat_consumer_manager = heartbeat_consumer_manager
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.heartbeat_manager = KafkaHeartbeatManager(producer_manager=self.producer_manager,
                                                       consumer_manager=self.heartbeat_consumer_manager,
                                                       )
        self.heartbeat_manager.start_heartbeat_check()

    def start(self):
        print("Starting Kafka handler...")
        self.consumer_manager.start()
        while True:
            msg = self.consumer_manager.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                self._handle_error(msg)
            else:
                self.executor.submit(self.process_task, msg)

    def process_task(self, msg):
        message = json.loads(msg.value())
        worker_id = str(uuid.uuid4())
        print(f"Processing task with worker ID: {worker_id}")

        # Reuse existing producer and consumer managers for heartbeat
        self.heartbeat_manager.start_heartbeat(worker_id=worker_id)
        # Simulate task processing
        time.sleep(5)  # Simulate task taking some time
        print(f"Task with worker ID: {worker_id} is completed.")

    def stop(self):
        print("Stopping Kafka handler...")
        self.executor.shutdown(wait=True)
        self.consumer_manager.stop()

    def _handle_error(self, msg):
        if msg.error().code() == KafkaError._PARTITION_EOF:
            sys.stderr.write(f'%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}\n')
        else:
            raise KafkaException(msg.error())


# Handler Factory
class HandlerFactory:
    @staticmethod
    def create_handler():
        broker_type = os.getenv('BROKER_TYPE')
        config = {'bootstrap.servers': os.getenv('BROKER_URL')}

        if broker_type == 'kafka':
            topic_name = 'task_flux_compacted'
            heartbeat_topic_name = 'task_flux_heartbeat'
            producer_manager = KafkaProducerManager(config, topic_name)
            consumer_manager = KafkaConsumerManager(config, topic_name)
            heartbeat_consumer_manager = KafkaConsumerManager(config, heartbeat_topic_name)
            return KafkaHandler(producer_manager, consumer_manager, heartbeat_consumer_manager)
        elif broker_type == 'redis':
            # Implement and return RedisHandler here
            pass
        elif broker_type == 'rabbitmq':
            # Implement and return RabbitMQHandler here
            pass
        else:
            raise ValueError(f"Unsupported broker type: {broker_type}")


# Main Program
if __name__ == "__main__":
    handler = HandlerFactory.create_handler()
    handler.start()
