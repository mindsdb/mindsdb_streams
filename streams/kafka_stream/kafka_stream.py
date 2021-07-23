import json
import kafka

from ..base import BaseStream


class KafkaStream(BaseStream):
    def __init__(self, topic, **connection_info):
        self.topic = topic
        self.producer = kafka.KafkaProducer(**connection_info, acks='all')
        self.consumer = kafka.KafkaConsumer(**connection_info, consumer_timeout_ms=100)
        self.consumer.subscribe(topics=[topic])

    def read(self):
        while True:
            try:
                msg = next(self.consumer)
                yield json.loads(msg.value)
            except StopIteration:
                break

    def write(self, dct):
        self.producer.send(self.topic, json.dumps(dct).encode('utf-8'))

    def __del__(self):
        self.consumer.close()
        self.producer.close()
