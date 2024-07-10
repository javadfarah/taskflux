from dispatchers import task_dispatcher
from config import env_config

# Kafka broker configuration
handler = env_config.HANDLER
handler_config = handler.handler_inputs()
task_flux = handler(**handler_config)


# Usage example with a regular function
@task_dispatcher()
def a(arg):
    print(arg)
    print("abbas")


# Synchronous call
# a.delay(arg="hello")

# Asynchronous call
# a.delay("unique_key_hello")


# Example with a static method
class MyClass:
    @task_dispatcher()
    @staticmethod
    def b(arg):
        print(arg)
        print("static method")


# Synchronous call
# MyClass.b.delay(b="abbas")


# Asynchronous call
# MyClass.b.delay("unique_key_static_call")


# Update data example
@task_dispatcher()
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
