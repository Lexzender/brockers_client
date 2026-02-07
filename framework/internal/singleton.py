import threading
from typing import Any


class Singleton:
    # Паттерн Singleton: на весь процесс создается только один объект класса.
    # Это полезно для общего состояния (например, кеш, конфиг).
    # Блокировка нужна для потокобезопасности: при одновременном вызове из
    # нескольких потоков второй объект не будет создан.
    _instance: Any | None = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__new__(cls, *args, **kwargs)
            return cls._instance
