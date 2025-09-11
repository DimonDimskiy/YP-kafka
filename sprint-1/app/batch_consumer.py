import json

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import MessageField, SerializationContext, StringDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

from .schemas import Message


class BatchMessageConsumer:
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
        print("BatchMessageConsumer запущен...")
        try:
            batch = []
            while True:
                msgs = self.consumer.consume(num_messages=10, timeout=3)
                if not msgs:
                    continue
                batch.extend(msgs)
                if len(batch) < 10:
                    continue
                batch_len = len(batch)
                processed = []
                while batch:
                    msg = batch.pop()
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            print(f"[ERROR] {msg.error()}")
                        continue
                    try:
                        key = self.key_deserializer(msg.key(), SerializationContext(self.topic, MessageField.KEY))
                        value = self.value_deserializer(msg.value(), SerializationContext(self.topic, MessageField.VALUE))
                        processed.append(f"[Batch] Сообщение: key={key}, value={value}")
                    except Exception as e:
                        print(f"[ERROR] Ошибка десериализации: {e}")

                # один коммит после обработки всей пачки
                self.consumer.commit(asynchronous=False)
                print("\n".join(processed))
                print(f"[Batch] Закоммитил оффсет после {batch_len} сообщений")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer_cfg = {
        "bootstrap.servers": "kafka-0:9092",     # приложение будет подключаться к докер сети потому адрес внутренний
        "group.id": "batch-message-consumer",    # группу объединяет общий оффсет и невозможность одновременно читать
                                                 # из одной партиции
        "auto.offset.reset": "latest",           # читаем с последнего сообщения если нет сохраненных коммитов
        "enable.auto.commit": False,             # ручной коммит
        "fetch.min.bytes": 2000,                 # ждём минимум 2KB ~ 10 сообщений
        "fetch.wait.max.ms": 3000                # или ждём до 3с
    }
    schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})
    consumer = BatchMessageConsumer(consumer_cfg, "my_topic", schema_registry_client)
    consumer.start_consuming()