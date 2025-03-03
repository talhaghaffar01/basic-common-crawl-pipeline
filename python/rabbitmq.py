from abc import ABC, abstractmethod
import os
import pika
import time
from typing import Optional

QUEUE_NAME = "batches"
MAX_RETRIES = 3
RETRY_DELAY = 2

class MessageQueueChannel(ABC):
    @abstractmethod
    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        pass


class RabbitMQChannel(MessageQueueChannel):
    def __init__(self) -> None:
        self.channel = None
        self.connect()
    
    def connect(self) -> None:
        try:
            self.channel = rabbitmq_channel()
        except Exception as e:
            print(f"Failed to connect to RabbitMQ: {e}")
            raise

    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        for attempt in range(MAX_RETRIES):
            try:
                if self.channel is None or self.channel.is_closed:
                    self.connect()
                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body,
                )
                return
            except (pika.exceptions.AMQPConnectionError,
                   pika.exceptions.AMQPChannelError,
                   ConnectionError) as e:
                print(f"Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                raise


def rabbitmq_channel() -> pika.adapters.blocking_connection.BlockingChannel:

    connection = pika.BlockingConnection(
        pika.URLParameters(os.environ["RABBITMQ_CONNECTION_STRING"])
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    return channel
