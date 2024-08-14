import sys
import signal
from config import env_config
from utils.functions import import_from_string

# Kafka broker configuration
handler = import_from_string(env_config.HANDLER)
handler_config = handler.handler_inputs(env_config.BROKER_URL,"default")
task_flux = handler(**handler_config)
#
#
# # Usage example with a regular function
# @task_dispatcher()
# def a(arg):
#     print(arg)
#     print("abbas")
#
#
# # Synchronous call
# # a.delay(arg="hello")
#
# # Asynchronous call
# # a.delay("unique_key_hello")
#
#
# # Example with a static method
# class MyClass:
#     @task_dispatcher()
#     @staticmethod
#     def b(arg):
#         print(arg)
#         print("static method")
#
#
# # Synchronous call
# # MyClass.b.delay(b="abbas")
#
#
# # Asynchronous call
# # MyClass.b.delay("unique_key_static_call")
#
#
# # Update data example
# @task_dispatcher()
# def update_data(key, new_data):
#     print(f"Updating key {key} with data: {new_data}")


# Initial data
# update_data.delay(key="unique_key", new_data={"field1": "value1"})
#
# # Updated data
# update_data.delay(key="unique_key", new_data={"field1": "new_value", "field2": "value2"})
#
# # Get latest data
# latest_data = handler.get_latest(key="unique_key")
# print("Latest data:", latest_data)
def handle_signal(signal_number, frame):
    print(f"Received signal {signal_number}, performing cleanup...")
    # Perform your cleanup or save state here
    # For example, close files, release resources, etc.
    task_flux.cleanup()
    sys.exit(0)


signal.signal(signal.SIGINT, handle_signal)  # Handle keyboard interrupt (Ctrl+C)
signal.signal(signal.SIGTERM, handle_signal)  # Handle termination signal
