import json
import queue
import threading

from kafka import KafkaConsumer


class Consumer:
    def __init__(self, bootstrap_servers=["185.185.143.231:9092"], topic: str = "register-events"):
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._consumer: KafkaConsumer | None = None
        self._running = threading.Event()  # флаг работаем/не работаем
        self._ready = threading.Event()  # Событие готовности фрнового потока-консумера (метод_consume). Означает что поток запустился и сообщил что он готов к работе
        self._thread: threading.Thread | None = None  # ссылка на фоновый поток
        self._messages: queue.Queue = queue.Queue()  # очередь,куда будут склыдываться сообщения

    def start(self):

        # Создаем kafka consumer
        self._consumer = KafkaConsumer(
            self._topic,
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
                """
                for topic_partition, records in messages.items():
                    for record in records:  # итерируемся по списку сообщений полученных из topic_partition
                        print(f"{topic_partition}: {record}")  # печатаем каждое сообщение
                        self._messages.put(record)  # кладем в очередь сообщение

        except Exception as e:
            print(f"Error: {e}")

    # Метод для получения сообщения из очереди
    def get_message(self, timeout: int = 90):
        try:
            return self._messages.get(
                timeout=timeout)  # Пытаемся взять сообщение из очереди. если сообщение есть вернет его, если нет то после таймаута кинет ошибку

        except queue.Empty:
            raise AssertionError("Queue is empty")

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

        if self._consumer:  # Закрываем kafka consumer
            try:
                self._consumer.close(timeout_ms=2000)
                print("Stop consuming")

            except Exception as e:
                print(f"Error while closing consumer: {e}")

        del self._consumer
        del self._messages

        print("Consumer stopped")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
