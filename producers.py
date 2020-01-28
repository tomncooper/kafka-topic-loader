import uuid
import time

from typing import List

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError


def send_messages(bootstrap_servers: str, topics: List[str], interval: float):

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, acks=0, retries=0)

    keep_sending: bool = True

    print(f"Sending messages at a rate of {round(1.0/interval,2)} per second")

    try:

        while keep_sending:
            try:
                payload: bytes = str(uuid.uuid4()).encode("utf-8")
                for topic in topics:
                    producer.send(topic, payload)
            except KafkaTimeoutError:
                print("Unable to fetch metadata")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("")
        print("Shutdown signal received. Closing producer...")
        producer.close(timeout=5)
        print("Producer closed.")
