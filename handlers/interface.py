from abc import ABC, abstractmethod


class HandlerInterface(ABC):
    @abstractmethod
    def send(self, *args, **kwargs):
        pass

    @abstractmethod
    def get(self, *args, **kwargs):
        pass
