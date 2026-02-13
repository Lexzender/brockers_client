#Класс абстрактный для того чтобы жестко указать пользователю, что нужно переопределить что бы все работало
import queue
from abc import ABC, abstractmethod
from kafka.consumer.fetcher import ConsumerRecord
"""
subscriber.py это заготовка (база) для “подписчика” Kafka: она складывает входящие сообщения в очередь и потом выдает их по одному.
Очень просто (аналогия) Представь почтовый ящик:
1.handle_message(...) кладет письмо в ящик.
2.get_message(...) достает письмо из ящика.
3.Если долго нет писем, бросается ошибка.

Зачем class Subscriber(ABC)  наследовать от ABC ?

Чтобы сделать Subscriber именно шаблоном, а не готовым классом.
Главная причина:
1.
С ABC + @abstractmethod Python заставляет наследников реализовать обязательные части (здесь topic в framework/internal/kafka/subscriber.py:16).
2.
Если кто-то забудет topic, объект такого класса создать нельзя, ошибка будет сразу, а не позже в рантайме.
3.
Это защищает от “полурабочих” подписчиков и делает контракт класса явным.
Без ABC можно случайно создать Subscriber/наследника без topic, и проблемы всплывут позже и менее понятно.


Зачем нужен данный класс
1.Разделить прием сообщений и их чтение.
2.Упростить тесты/ожидание событий: “ждали N секунд, не пришло -> ошибка”.
3.Сделать единый интерфейс для разных подписчиков (через обязательный topic).

В Subscriber разделение сделано так:
1.Прием сообщений: handle_message(...) в framework/internal/kafka/subscriber.py:19
Этот метод только принимает record и кладет в очередь: self._messages.put(record).

2.Чтение сообщений: get_message(...) в framework/internal/kafka/subscriber.py:23
Этот метод только читает из очереди: self._messages.get(timeout=timeout).

То есть:
•один “вход” (handle_message) складывает данные,
•другой “выход” (get_message) достает данные.

Зачем это полезно:
•код, который получает события из Kafka, не обязан сразу их обрабатывать;
код, который ждет событие (например в тесте), берет его позже через get_message.
А очередь _messages в framework/internal/kafka/subscriber.py:12 и есть “буфер” между этими двумя этапами.

"""


# В классе Subscriber будем хранить только те сообщения которые принадлежат именно ему
class Subscriber(ABC):
    """
    Интерфейс подписки
    """
    def __init__(self):
        self._messages: queue.Queue = queue.Queue()

    @property
    @abstractmethod   #@abstractmethod Помечает метод как обязательный для наследников. Если в дочернем классе его не реализовать, объект создать нельзя.
    def topic(self) -> str:...
    """
    Каждый конкретный подписчик обязан сказать, какой у него топик (str).
    Если не реализовать topic, класс-наследник будет “недоделан”.
    """

    def handle_message(self, record: ConsumerRecord) -> None:
        self._messages.put(record)

    # Метод для получения сообщения из очереди
    def get_message(self, timeout: int = 90):
        try:
            return self._messages.get(
                timeout=timeout)  # Пытаемся взять сообщение из очереди. если сообщение есть вернет его, если нет то после таймаута кинет ошибку

        except queue.Empty:
            raise AssertionError(f"No messages from topic: {self.topic}, with timeout: {timeout}")
