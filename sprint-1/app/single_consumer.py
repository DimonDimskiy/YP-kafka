import json

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import MessageField, SerializationContext, StringDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

from .schemas import Message


class SingleMessageConsumer:
    def __init__(self, kafka_cfg, topic_name, schema_registry_client):
        self.topic = topic_name
        self.consumer = Consumer(kafka_cfg)
        self.key_deserializer = StringDeserializer("utf-8")
        self.value_deserializer = JSONDeserializer(
            schema_str=json.dumps(Message.model_json_schema()),
            schema_registry_client=schema_registry_client,
            from_dict=lambda d, ctx: Message.model_validate(d)
        )

    def start_consuming(self):
        self.consumer.subscribe([self.topic])
        print("SingleMessageConsumer запущен...")
        try:
            while True:
                msg = self.consumer.poll()
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[ERROR] {msg.error()}")
                    continue
                try:
                    key = self.key_deserializer(msg.key(), SerializationContext(self.topic, MessageField.KEY))
                    value = self.value_deserializer(msg.value(), SerializationContext(self.topic, MessageField.VALUE))
                    print(f"[Single] Получено сообщение: key={key}, value={value}")
                except Exception as e:
                    print(f"[ERROR] Ошибка десериализации: {e}")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer_cfg = {
        "bootstrap.servers": "kafka-0:9092",
        "group.id": "single-message-consumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True
    }
    schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})
    consumer = SingleMessageConsumer(consumer_cfg, "my_topic", schema_registry_client)
    consumer.start_consuming()
