import time
import uuid

import pytest

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.helpers.kafka.consumers.register_events_errors import RegisterEventsErrorsSubscriber
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


@pytest.fixture
def register_message() -> dict[str, str]:
    base = uuid.uuid4().hex
    return {"login": base,
            "email": f"{base}@mail.ru",
            "password": "1234541244"}


def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_mail = "string@mail.ru"
    account.register_user(login="string", email=expected_mail, password="string")

    for _ in range(10):
        response = mail.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError("email found")


def test_success_registration(register_events_subscriber: RegisterEventsSubscriber,
                              register_message: dict[str, str],
                              account: AccountApi,
                              mail: MailApi) -> None:
    login = register_message["login"]

    account.register_user(**register_message)
    register_events_subscriber.find_message(login=login)

    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("No mail found")


def test_success_registration_with_kafka_producer(register_message: dict[str, str],
                                                  mail: MailApi,
                                                  kafka_producer: Producer) -> None:
    login = register_message["login"]

    kafka_producer.send('register-events', register_message)

    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("No mail found")


def test_register_events_error_consumer(account: AccountApi, mail: MailApi, kafka_producer: Producer) -> None:
    base = uuid.uuid4().hex
    message = {
        "input_data": {
            "login": base,
            "email": f"{base}@mail.ru",
            "password": "1234541244",
        },
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
            "errors": {
                "Email": [
                    "Invalid",
                ],
            },
        },
        "error_type": "unknown",
    }

    kafka_producer.send('register-events-errors', message)

    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("No mail found")

    confirmation_id = mail.extract_confirmation_id(query=base)
    account.activate_user(confirmation_id, login=base)


def test_success_registration_with_kafka_producer_consumer(register_message: dict[str, str],
                                                           register_events_subscriber: RegisterEventsSubscriber,
                                                           kafka_producer: Producer) -> None:
    login = register_message["login"]

    kafka_producer.send('register-events', register_message)
    message = register_events_subscriber.get_message()
    if message.value["login"] == login:
        return
    else:
        raise AssertionError("No mail found")


def test_failed_registration_with_kafka_consumer(register_events_subscriber: RegisterEventsSubscriber,
                                                 register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
                                                 account: AccountApi,
                                                 mail: MailApi
                                                 ) -> None:
    expected_mail = "string@mail.ru"
    login = 'aleksey'
    account.register_user(login=login, email=expected_mail, password="aleksey")

    register_events_subscriber.find_message(login)
    register_events_errors_subscriber.find_message_with_error_type_validation(login)


def test_republish_unknown_error_as_validation(register_events_errors_subscriber: RegisterEventsErrorsSubscriber,
                                               kafka_producer: Producer
                                               ) -> None:
    login = 'aleksey'
    message = {
        "input_data": {
            "login": f"{login}",
            "email": "string@mail.ru",
            "password": "aleksey",
        },
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
            "errors": {
                "Email": [
                    "Invalid",
                ],
            },
        },
        "error_type": "unknown",
    }

    kafka_producer.send('register-events-errors', message)

    register_events_errors_subscriber.find_message_with_error_type_validation(login)
