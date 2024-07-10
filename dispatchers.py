from task import Task
from main import task_flux


def task_dispatcher():
    def decorator(func):
        def wrapper(*args, **kwargs):
            task_instance = Task(task_flux, func, *args, **kwargs)
            return task_instance

        wrapper.delay = lambda *args, **kwargs: Task(task_flux, func, *args, **kwargs).delay()
        wrapper.run = lambda *args, **kwargs: func(*args, **kwargs)
        return wrapper

    return decorator
