import queue
from abc import ABC, abstractmethod
from kafka.consumer.fetcher import ConsumerRecord


class Subscriber(ABC):

    def __init__(self):
        self._messages: queue.Queue = queue.Queue()

    @property
    @abstractmethod
    def topic(self) -> str:
        ...

    def handle_message(self, record: ConsumerRecord) -> None:
        self._messages.put(record)

    def get_message(self, timeout: int = 90):
        try:
            return self._messages.get(
                timeout=timeout)

        except queue.Empty:
            raise AssertionError(f"No messages from topic: {self.topic}, with timeout: {timeout}")
