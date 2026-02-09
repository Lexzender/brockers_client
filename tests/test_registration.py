import json
import time
import uuid
from kafka import KafkaProducer
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_mail = "string@mail.ru"
    account.register_user(login="string", email=expected_mail, password="string")

    for _ in range(10):
        response = mail.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError("email found")


def test_success_registration(account: AccountApi, mail: MailApi) -> None:
    base = uuid.uuid4().hex
    account.register_user(login=base, email=f"{base}@mail.ru", password="1234541244")

    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("No mail found")


def test_success_registration_with_kafka_producer(mail: MailApi, kafka_producer: Producer) -> None:

    base = uuid.uuid4().hex
    message = {"login": base,
               "email": f"{base}@mail.ru",
               "password": "1234541244"}

    kafka_producer.send('register-events', message)

    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("No mail found")

def test_register_events_error_consumer(account: AccountApi, mail: MailApi,kafka_producer: Producer) -> None:
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

    confirmation_id = mail.find_confirmation_id(query=base)
    account.activate_user(confirmation_id,login=base)
