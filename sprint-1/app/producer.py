import time
import json

from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

from .schemas import Message


class MyProducer:
    def __init__(self, kafka_cfg, topic_name, schema_registry_client):
        self.producer = Producer(kafka_cfg)
        self.topic = topic_name
        self.key_serializer = StringSerializer("utf-8")
        self.value_serializer = JSONSerializer(
            schema_str=json.dumps(Message.model_json_schema()),
            schema_registry_client=schema_registry_client
        )

    def start_producing(self):
        fake = Faker("ru_RU")
        while True:
            random_sentence = fake.sentence()
            key = random_sentence[0]
            message = Message(msg=random_sentence)
            print(f"[Producer]: отправляю сообщение: {message}")
            self.producer.produce(
                topic=self.topic,
                key=self.key_serializer(key, SerializationContext(self.topic, MessageField.KEY)),
                value=self.value_serializer(
                    message.model_dump(),
                    SerializationContext(self.topic, MessageField.VALUE)),
            )
            self.producer.flush()
            time.sleep(0.5)


if __name__ == "__main__":
    producer_cfg = {
        "bootstrap.servers": "kafka-0:9092",
        "acks": "all" # ждем пока все синхронизированные реплики подтвердят получение
    }
    schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})
    producer = MyProducer(producer_cfg, "my_topic", schema_registry_client)
    producer.start_producing()