import json
import time
import uuid
from kafka import KafkaProducer
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi


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


def test_success_registration_with_kafka_producer(mail: MailApi) -> None:
    '''
    bootstrap_servers - адрес хоста кафки. Передавать лучше листом. Так как в кластере может быть много брокеров
    acks = "all" ждем подтвержение от каждой реплики, что сообщение было отправлено

    Возможные варианты:
    1)acks=0
    Продюсер ничего не ждёт.Он просто отправил и «забыл».Быстро, но если Kafka не приняла сообщение — ты об этом не узнаешь.
    2)acks=1
    Продюсер ждёт подтверждение только от лидера партиции.Лидер записал сообщение в свой локальный лог — и подтвердил.
    Если лидер упадёт до того, как реплики скопировали данные — можно потерять сообщение.
    3)acks=all (или -1)
    Продюсер ждёт подтверждение от всех in-sync реплик (ISR).
    Это значит: лидер записал сообщение и дождался, пока все «живые» реплики его тоже запишут.
    Самый надёжный вариант, но медленнее.

    retries - сколько раз пробовать повторно отправить сообщение при временных ошибках
    retry_backoff_ms - пауза между повторами отправки (5 секунд
    request_timeout_ms - сколько максимум ждать ответа на сетевой запрос (70 секунд). т.е сколько ms ждем от лидера
     партиции подверждение того, что сообщение было доставлено

    connection_max_idle_ms - через сколько закрывать «простаивающее» соединение (65 секунд).
    reconnect_backoff_ms - пауза перед повторной попыткой переподключения
    reconncet_bacjoff_max_ms - верхний предел паузы между попытками переподключения (10 секунд).
    value_serializer - перевод сообщения к байтовой строке
    '''

    base = uuid.uuid4().hex
    message = {"login": base,
               "email": f"{base}@mail.ru",
               "password": "1234541244"}


    producer = KafkaProducer(bootstrap_servers=["185.185.143.231:9092"],
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                             acks="all",
                             retries=5,
                             retry_backoff_ms=5000,
                             request_timeout_ms=70000,
                             connections_max_idle_ms=65000,
                             reconnect_backoff_ms=5000,
                             reconnect_backoff_max_ms=10000)

    producer.send('register-events', message)
    producer.close()

    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("No mail found")

