from framework.internal.kafka.subscriber import Subscriber

"""
Это “подписчик на топик register-events”.
т.е создали подписчика и указали какой топик он будет читать
"""

class RegisterEventsSubscriber(Subscriber):
    topic: str = "register-events"