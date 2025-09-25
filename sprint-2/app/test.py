import unittest
import json
import time

from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.serialization import StringSerializer, StringDeserializer

from app import config

def json_deserializer(obj, ctx):
    return json.loads(obj)

def json_serializer(obj, ctx):
    return json.dumps(obj).encode("utf-8")

class TestFaustApp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.producer = SerializingProducer({
            "bootstrap.servers": config.BROKER,
            "key.serializer": StringSerializer(),
            "value.serializer": json_serializer
        })
        cls.consumer = DeserializingConsumer({
            "bootstrap.servers": config.BROKER,
            "group.id": "test-consumer",
            "auto.offset.reset": "earliest",
            "key.deserializer": StringDeserializer(),
            "value.deserializer": json_deserializer
        })
        cls.consumer.subscribe([config.FILTERED_MSG_TOPIC])
        time.sleep(3)
        while True:
            msg = cls.consumer.poll(1)
            if msg is None:
                break

    @classmethod
    def tearDownClass(cls):
        cls.consumer.close()

    def send(self, topic, value, key=None):
        self.producer.produce(topic, key=key, value=value)
        self.producer.flush()

    def poll_result(self, timeout=5.0):
        end = time.time() + timeout
        while time.time() < end:
            msg = self.consumer.poll(3.0)
            if msg is None or msg.error():
                continue
            self.consumer.commit(msg)
            return msg.value()
        return None

    # --- Тесты функций ---
    def test_word_filtering(self):
        """Сообщение с запрещённым словом должно быть отфильтровано,
        список запрещенных слов должен редактироваться"""

        # Блокируем слова
        self.send(config.FORBIDDEN_WORDS_TOPIC, {"value": "реклама", "unblock": False})
        self.send(config.FORBIDDEN_WORDS_TOPIC, {"value": "спам", "unblock": False})

        time.sleep(3)
        self.send(
            config.RAW_MSG_TOPIC,
            {"sender": "test_1", "recipient": "eduard", "value": "Это сообщение СПАМ и реклама"},
        )
        result = self.poll_result()
        self.assertIsNotNone(result)
        self.assertEqual("Это сообщение *** и ***", result["value"])

        # Убираем "спам" из списка запрещенных
        self.send(config.FORBIDDEN_WORDS_TOPIC, {"value": "спам", "unblock": True})

        time.sleep(3)
        self.send(
            config.RAW_MSG_TOPIC,
            {"sender": "test_1", "recipient": "eduard", "value": "Это сообщение СПАМ и реклама"},
        )
        result = self.poll_result()
        self.assertIsNotNone(result)
        self.assertEqual( "Это сообщение СПАМ и ***", result["value"])

    def test_user_blocking(self):
        """Сообщения от заблоченных пользователей не должны доходить"""

        # Сообщение от незаблокированного пользователя должно пройти, разблокируем на всякий случай
        # вдруг упало на втором кейсе
        self.send(config.BLOCKED_USERS_TOPIC, {"initiator": "eduard", "target": "test_2", "unblock": True})
        time.sleep(3)
        self.send(
            config.RAW_MSG_TOPIC,
            {"sender": "test_2", "recipient": "eduard", "value": "Это сообщение от пока незаблоченного юзера"},
        )

        result = self.poll_result()
        self.assertIsNotNone(result)
        self.assertEqual(result["value"], "Это сообщение от пока незаблоченного юзера")

        # Блокируем test_2
        self.send(config.BLOCKED_USERS_TOPIC, {"initiator": "eduard", "target": "test_2", "unblock": False})
        time.sleep(3)
        # Теперь сообщение не должно пройти
        self.send(
            config.RAW_MSG_TOPIC,
            {"sender": "test_2", "recipient": "eduard", "value": "Это сообщение от заблоченного юзера"},
        )
        result = self.poll_result()
        self.assertIsNone(result)

        # Сообщение от разблокированного пользователя должно пройти
        self.send(config.BLOCKED_USERS_TOPIC, {"initiator": "eduard", "target": "test_2", "unblock": True})
        self.send(
            config.RAW_MSG_TOPIC,
            {"sender": "test_2", "recipient": "eduard", "value": "Это сообщение от разблокированного юзера"},
        )
        time.sleep(3)
        result = self.poll_result()
        self.assertIsNotNone(result)
        self.assertEqual(result["value"], "Это сообщение от разблокированного юзера")
