import unittest
import json
import time
import os
import subprocess
import logging

from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer


logger = logging.getLogger(__name__)
logger.setLevel("INFO")

load_dotenv()
BROKERS = os.getenv("BROKERS")

# --- Тесты ---
class TestFaustApp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.producer = Producer({"bootstrap.servers": BROKERS})
        cls.consumer =  Consumer({
            "bootstrap.servers": BROKERS,
            "group.id": "test-reader",
            "auto.offset.reset": "latest"
        })
        cls.consumer.subscribe(topics=[
            "filtered_messages",
            "msg-filter-blocked-users-changelog",
            "msg-filter-forbidden-words-changelog"
        ])

    def send(self, topic, value, key=None):
        self.producer.produce(
            topic,
            key=key.encode("utf-8") if key else None,
            value=json.dumps(value).encode("utf-8")
        )
        self.producer.flush()

    def poll_result(self, expected_key=None, timeout=5):
        end = time.time() + timeout
        while time.time() < end:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            k = msg.key().decode("utf-8") if msg.key() else None
            v = json.loads(msg.value().decode())
            if expected_key is None or k == expected_key:
                return v
        return None

    # --- Тесты функций ---
    def test_word_filtering(self):
        """Сообщение с запрещённым словом должно быть отфильтровано"""
        self.send("forbidden_words", {"value": "спам", "unblock": False})
        result = self.poll_result("спам")
        logger.info(f"Polling changelog result: {result}")
        time.sleep(5)
        key = "test_word_filtering"
        self.send(
            "messages",
            {"sender": "test_1", "recipient": "eduard", "value": "Это СПАМ сообщение"},
            key=key
        )
        result = self.poll_result(expected_key=key)
        self.assertIsNotNone(result)
        self.assertEqual(result["value"], "Это *** сообщение")


if __name__ == "__main__":
    unittest.main()
