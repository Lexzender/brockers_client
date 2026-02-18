from framework.internal.kafka.subscriber import Subscriber


class RegisterEventsErrorsSubscriber(Subscriber):
    topic: str = "register-events-errors"

    def find_message_with_error_type_validation(self, login):
        for i in range(10):
            message = self.get_message()
            if message.value["input_data"]["login"] == login and message.value["error_type"] == "validation":
                break
        else:
            raise AssertionError("No mail found")