from framework.internal.kafka.subscriber import Subscriber

"""
Это “подписчик на топик register-events”.
т.е создали подписчика и указали какой топик он будет читать
"""

class RegisterEventsSubscriber(Subscriber):
    topic: str = "register-events"

    def find_message(self,login):
        for i in range(10):
            message = self.get_message()
            if message.value["login"] == login:
                break
        else:
            raise AssertionError("No mail found")