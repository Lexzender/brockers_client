import pytest

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.consumer import Consumer
from framework.internal.kafka.producer import Producer


@pytest.fixture(scope="session")
def account() -> AccountApi:
    return AccountApi()

@pytest.fixture(scope="session")
def mail()->MailApi:
    return MailApi()

@pytest.fixture(scope="session")
def kafka_producer() -> Producer:
    with Producer() as producer:
        yield producer


#Создаем подписчика в фикстуре
@pytest.fixture(scope="session")
def register_events_subscriber() -> RegisterEventsSubscriber:
    return RegisterEventsSubscriber()
"""
1.Создается RegisterEventsSubscriber.
2.Передается в kafka_consumer.
3.Consumer получает список подписчиков при инициализации.
4.После входа в with делает регистрацию и стартует чтение Kafka.
"""


@pytest.fixture(scope="session", autouse=True)
def kafka_consumer(register_events_subscriber:RegisterEventsSubscriber) -> Consumer:
    with Consumer(subscribers=[register_events_subscriber]) as consumer:
        yield consumer


"""
2.
Сначала вызывается конструктор Consumer(...) -> __init__
Там сохраняется self._subscribers = subscribers (framework/internal/kafka/consumer.py:19).
3.
После успешного создания объекта Python автоматически вызывает __enter__ (framework/internal/kafka/consumer.py:127).
4.
В __enter__ по очереди:
•
self.register() (framework/internal/kafka/consumer.py:128)
•
self.start() (framework/internal/kafka/consumer.py:129)
"""





"""
При входе в блок with мы запускаем контекстный менеджер, который в consumer вызывает метод .__enter__


В Consumer.__init__ сохраняется список подписчиков в self._subscribers
framework/internal/kafka/consumer.py:17-19.

В __enter__ вызывается self.register()
framework/internal/kafka/consumer.py:127-129.

В register() идет цикл по подписчикам и заполняется self._watchers: framework/internal/kafka/consumer.py:33-35

for subscriber in self._subscribers:
    self._watchers[subscriber.topic].append(subscriber)
    
То есть ключи self._watchers это и есть subscriber.topic.
self._watchers = {
  "register-events": [register_events_subscriber]
}


Потом в start() берутся эти ключи: framework/internal/kafka/consumer.py:41

*self._watchers.keys()


И они передаются в KafkaConsumer(...) как список топиков для прослушивания.
Пример в твоем проекте:
1.Есть RegisterEventsSubscriber с topic = "register-events" (framework/helpers/kafka/consumers/register_events.py:5).
2.После register() в self._watchers будет ключ "register-events".
3.Значит KafkaConsumer начнет слушать именно этот топик.
"""