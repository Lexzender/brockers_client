import json
import queue
import threading
import time
from collections import defaultdict

from kafka import KafkaConsumer

from framework.internal.kafka.subscriber import Subscriber
from framework.internal.singleton import Singleton


class Consumer(Singleton):

    _started: bool = False

    def __init__(self, subscribers: list[Subscriber],bootstrap_servers=["185.185.143.231:9092"]):
        self._bootstrap_servers = bootstrap_servers
        self._subscribers = subscribers   # список подписчиков
        self._consumer: KafkaConsumer | None = None
        self._running = threading.Event()  # флаг работаем/не работаем
        self._ready = threading.Event()  # Событие готовности фрнового потока-консумера (метод_consume). Означает что поток запустился и сообщил что он готов к работе
        self._thread: threading.Thread | None = None  # ссылка на фоновый поток
        self._watchers: dict[str, list[Subscriber]] = defaultdict(list)   # создаем список подписчиков пример { "register-events": [register_events_subscriber], "register-events-errors": [another_subscriber]}

    def register(self):
        if self._subscribers is None:
            raise RuntimeError("Subscribers is not initialized")

        if self._started:
            raise RuntimeError("Consumer is already started")

        for subscriber in self._subscribers:
            print(f"Registering subscriber {subscriber.topic}")
            self._watchers[subscriber.topic].append(subscriber)

    def start(self):

        # Создаем kafka consumers
        self._consumer = KafkaConsumer(
            *self._watchers.keys(),  # сразу подключаемся ко всем топикам
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self._running.set()  # Включаем флаг _running, который означает что консумер создан
        self._ready.clear()  # Перед стартом обнуляется старый флаг готовности
        # Создается объект потока (но еще не запускается).
        # target=self._consume = в этом потоке будет выполняться метод _consume
        # daemon=True = поток “служебный”: если основной процесс завершится, Python не будет ждать этот поток бесконечно.
        self._thread = threading.Thread(target=self._consume, daemon=True)
        self._thread.start()  # Запускаем фоновый поток _consume в котором будет идти чтение кафки параллельно, не блокируя основной код

        if not self._ready.wait(timeout=10):  # тут ожидаем что в течение 10 сек _consume будет готов к работе,
            # если не готов бросаем ошибку  RuntimeError
            raise RuntimeError("Consumer not ready yet")

        self._started = True

    # Бесконечный цикл чтения сообщения
    def _consume(self):

        self._ready.set()  # После старта функции _consume устанавливаем флаг о том что consume готов к работе
        print("Consumer started")
        # далее в цикле читаем кафку
        # каждое сообщение принтуем
        # каждое сообщение клладем в очередь _messages
        # если случилась ошибка отлавливаем и печатаем
        try:
            while self._running.is_set():
                messages = self._consumer.poll(timeout_ms=1000,
                                               max_records=10)  # Пытаемся забрать сообщение. Ждем максимум 1 сек, за один вызов вернет не больше 10 сообщений (max_records). Если ничего не пришло, то вернет пустой словарь
                """
                timeout_ms=1000
                чтобы поток не зависал навсегда и регулярно проверял, не пора ли остановиться.
            
            
                max_records=10
                чтобы читать сообщения порциями (батчами), а не слишком много за раз. Это снижает нагрузку и делает обработку более управляемой.
                
                пример того что будет в messages
                {
                TopicPartition(topic='register-events', partition=0): [record1, record2],  (record - сообщение)
                TopicPartition(topic='register-events-errors', partition=0): [record3]
                }
                """
                for topic_partition, records in messages.items():
                    topic = topic_partition.topic
                    for record in records:
                        for watcher in self._watchers[topic]:
                            print(f"{topic}: {record}")
                            watcher.handle_message(record)
                    time.sleep(0.1)

                if not messages:
                    time.sleep(0.1)



        except Exception as e:
            print(f"Error: {e}")


    # метод для остановки консумера
    def stop(self):
        self._running.clear()  # Устанавливаем флаг "выкл". Внутри _consume цикл видит, что флаг выключили и
        # заканчивает свою работу. Когда функция доходит до конца и завершается в этот момент автоматически завершается поток.
        # join(timeout=2) позволяет нам дождаться коректно завершения потока, а не обрубать его. Поток может быть еше
        # живым так как обрабатывает пачку сообщений или завис

        if self._thread and self._thread.is_alive():  # ждем завершение потока
            self._thread.join(timeout=2)
            if self._thread.is_alive():
                print("Thread is still alive")

        if self._consumer:  # Закрываем kafka consumers
            try:
                self._consumer.close(timeout_ms=2000)
                print("Stop consuming")

            except Exception as e:
                print(f"Error while closing consumers: {e}")

        del self._consumer
        self._watchers.clear()
        self._subscribers.clear()
        self._started = False


        print("Consumer stopped")

    def __enter__(self):
        self.register()
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()



"""
Как тест достает сообщение обратно
Что происходит:
1.Тест отправляет сообщение через kafka_producer.send('register-events', message).
2.Через некоторое время курьер (Consumer) подберет его из Kafka и положит в ящик подписчика.
3.Тест вызывает register_events_subscriber.get_message().
4.get_message() достает из очереди первое письмо.
5.Тест проверяет: message.value["login"] == base.

Бытовой пример:
1.Ты сам отправил письмо на почту.
2.Курьер принес его в твой ящик.
3.Ты открыл ящик и проверил, что это именно твое письмо по имени/логину.
Самая короткая схема:
1.send() = “отправить письмо на почту”.
2.Consumer.poll() = “курьер забрал с почты”.
3.handle_message() = “курьер положил в ящик”.
4.get_message() = “ты достал из ящика”
"""