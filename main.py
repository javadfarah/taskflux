
from dispatchers import task_dispatcher
from handlers.kafka_handler import KafkaHandler





# Kafka broker configuration
conf = {
    'bootstrap.servers': 'kafka-0.kafka-headless.python.svc.trader-stage.charisma.tech:30121,kafka-1.kafka-headless.python.svc.trader-stage.charisma.tech:30122,kafka-2.kafka-headless.python.svc.trader-stage.charisma.tech:30123',
}
handler = KafkaHandler(config=conf, topic_name="task_flux_compacted")


# Usage example with a regular function
@task_dispatcher(handler)
def a(arg):
    print(arg)
    print("abbas")


# Synchronous call
# a.delay(arg="hello")

# Asynchronous call
# a.delay("unique_key_hello")


# Example with a static method
class MyClass:
    @task_dispatcher(handler)
    @staticmethod
    def b(arg):
        print(arg)
        print("static method")


# Synchronous call
# MyClass.b.delay(b="abbas")


# Asynchronous call
# MyClass.b.delay("unique_key_static_call")


# Update data example
@task_dispatcher(handler)
def update_data(key, new_data):
    print(f"Updating key {key} with data: {new_data}")

# Initial data
# update_data.delay(key="unique_key", new_data={"field1": "value1"})
#
# # Updated data
# update_data.delay(key="unique_key", new_data={"field1": "new_value", "field2": "value2"})
#
# # Get latest data
# latest_data = handler.get_latest(key="unique_key")
# print("Latest data:", latest_data)
