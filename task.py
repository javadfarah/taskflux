import json
import uuid
import inspect
import os
from types import FunctionType
from handlers.interface import HandlerInterface
from utils.functions import import_from_string


class Task:
    def __init__(self, handler: HandlerInterface, func: FunctionType, *args, **kwargs):
        self.handler = handler
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.task_path = self._get_task_path(func)

    @staticmethod
    def _build_task_data(task_path, args, kwargs):
        data = dict(task_path=task_path, args=args, kwargs=kwargs)
        data = json.dumps(data, indent=2).encode('utf-8')
        return data

    @staticmethod
    def _get_task_path(func: FunctionType):
        if isinstance(func, staticmethod):
            func = func.__func__
        filename = inspect.getfile(func)
        filename = os.path.basename(filename).replace('.py', '')
        qualified_name = func.__qualname__
        return f"{filename}.{qualified_name}"

    def delay(self):
        key = uuid.uuid4()
        self._apply_to_broker(key)
        return key

    @staticmethod
    def run_task(task_path: str, args: list, kwargs: dict):
        task = import_from_string(task_path)
        task.run(*args, **kwargs)

    def _apply_to_broker(self, key):
        task_data = self._build_task_data(task_path=self.task_path, args=self.args, kwargs=self.kwargs)
        self.handler.send(task_data=task_data, key=key)
