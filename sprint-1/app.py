import uuid
from datetime import datetime
import json
import time

from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer, StringDeserializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

from pydantic import BaseModel, Field
from faker import Faker


class Message(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    msg: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class MyProducer(Producer):
    def __init__(self, kafka_cfg, topic_name, schema_registry_client):
        super().__init__(kafka_cfg)
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
            print(f"Отправляю: {message}")
            self.produce(
                topic=self.topic,
                key=self.key_serializer(key, SerializationContext(self.topic, MessageField.KEY)),
                value=self.value_serializer(
                    message.model_dump(),
                    SerializationContext(self.topic, MessageField.VALUE)),
            )
            self.flush()
            time.sleep(0.5)







if __name__ == "__main__":
    import multiprocessing
    import signal
    import sys

    producer_cfg = {
        "bootstrap.servers": "localhost:9094",
        "acks": "all",
        "retries": 3,
    }
    consumer_cfg = {
        "bootstrap.servers": "localhost:9094"
    }
    schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})


    def run_producer():
        producer = MyProducer(producer_cfg, "my_topic", schema_registry_client)
        try:
            producer.start_producing()
        except KeyboardInterrupt:
            print("[Producer] Остановлен вручную")


    def run_single_consumer():
        consumer = SingleMessageConsumer(consumer_cfg, "my_topic", schema_registry_client)
        try:
            consumer.start_consuming()
        except KeyboardInterrupt:
            print("[SingleConsumer] Остановлен вручную")
            consumer.consumer.close()


    def run_batch_consumer():
        consumer = BatchMessageConsumer(consumer_cfg, "my_topic", schema_registry_client)
        try:
            consumer.start_consuming()
        except KeyboardInterrupt:
            print("[BatchConsumer] Остановлен вручную")
            consumer.consumer.close()



    processes = []

    # создаём процессы
    processes.append(multiprocessing.Process(target=run_producer, name="Producer"))
    processes.append(multiprocessing.Process(target=run_single_consumer, name="SingleConsumer"))
    processes.append(multiprocessing.Process(target=run_batch_consumer, name="BatchConsumer"))

    # запускаем все процессы
    for p in processes:
        p.start()


    # функция для graceful shutdown
    def shutdown(signum, frame):
        print("\n[Main] Остановка всех процессов...")
        for p in processes:
            p.terminate()  # посылаем SIGTERM
            p.join()
        sys.exit(0)


    # ловим Ctrl+C и SIGTERM
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ждём завершения
    for p in processes:
        p.join()
