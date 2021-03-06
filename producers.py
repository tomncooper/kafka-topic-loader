import uuid
import time
import random
import logging

from typing import List, Union, Dict

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaTimeoutError

from topics import sort_partitions_by_leader_node

LOG: logging.Logger = logging.getLogger("kafka-topic-loader.producers")


def send_messages(
    bootstrap_servers: Union[str, List[str]], interval: float
):

    LOG.info("Creating Kafka Admin Client")
    admin_client: KafkaAdminClient = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers, client_id="topic-loader-admin"
    )

    # Get the mapping from topic to node id to partitions with leader replicas on that
    # node
    tpln: Dict[str, Dict[int, List[int]]] = sort_partitions_by_leader_node(admin_client)

    # Get a list of all node ids in the cluster
    LOG.info("Getting existing Kafka node list")
    nodes: List[int] = [node.nodeId for node in admin_client._client.cluster.brokers()]

    LOG.info("Creating Kafka Producer")
    producer: KafkaProducer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="test-loader-sender",
        acks=0,
        retries=0,
    )

    keep_sending: bool = True

    LOG.info("Loading %d topics", len(tpln.keys()))

    try:

        while keep_sending:

            payload: bytes = str(uuid.uuid4()).encode("utf-8")

            topic: str
            node_partiton_leaders: Dict[int, List[int]]

            for topic, node_partiton_leaders in tpln.items():
                multiplier: int = 1
                partitions: List[int]
                for partitions in node_partiton_leaders.values():
                    # For each node cycle through the partitions whose leaders on are
                    # that node
                    partition: int
                    for partition in partitions:
                        # For each partition send a number of messages depending on
                        # how far down the node list we are
                        for _ in range(multiplier):
                            try:
                                producer.send(
                                    topic=topic, value=payload, partition=partition
                                )
                            except KafkaTimeoutError:
                                LOG.error("Unable to fetch metadata")
                    # Send more messages to the next node in the list
                    multiplier += 1

            time.sleep(interval)

    except KeyboardInterrupt:
        print("Shutdown signal received. Closing producer...")
        producer.close(timeout=5)
        print("Producer closed.")
