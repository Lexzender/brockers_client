from framework.internal.kafka.subscriber import Subscriber


class RegisterEventsSubscriber(Subscriber):
    topic: str = "register-events"

    def find_message(self, login):
        for i in range(10):
            message = self.get_message()
            if message.value["login"] == login:
                break
        else:
            raise AssertionError("No mail found")
