from task import Task


def task_dispatcher(handler):
    def decorator(func):
        def wrapper(*args, **kwargs):
            task_instance = Task(handler, func, *args, **kwargs)
            return task_instance

        wrapper.delay = lambda *args, **kwargs: Task(handler, func, *args, **kwargs).delay()
        wrapper.run = lambda *args, **kwargs: func(*args,**kwargs)
        return wrapper

    return decorator
