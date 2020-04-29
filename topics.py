import logging
import math

import datetime as dt

from collections import defaultdict
from typing import List, Union, Set, Dict, DefaultDict, Optional

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.protocol.admin import Response
from kafka.structs import PartitionMetadata

LOG: logging.Logger = logging.getLogger("kafka-topic-loader.topics")

EXCLUDED_TOPICS = ["__", "strimzi"]


def get_partition_metadata(
    admin_client: KafkaAdminClient,
) -> Dict[str, Dict[int, PartitionMetadata]]:

    return admin_client._client.cluster._partitions


def sort_partitions_by_leader_node(
    admin_client: KafkaAdminClient,
) -> Dict[str, Dict[int, List[int]]]:
    """ This method produces a dictionary which maps from topic string to node id to a
    list of partition ids for those partitions whose lead replica is on that node.
    """

    LOG.info("Sorting partitions by leader nodes")

    partition_metadata: PartitionMetadata = get_partition_metadata(admin_client)

    loader_topics: List[str] = get_all_loader_topics(admin_client=admin_client)

    tnp: Dict[str, Dict[int, List[int]]] = {}

    for topic, partition_dict in partition_metadata.items():
        if topic in loader_topics:
            node_partiton_leader: DefaultDict[int, List[int]] = defaultdict(list)
            for partition, pmd in partition_dict.items():
                node_partiton_leader[pmd.leader].append(partition)
            tnp[topic] = dict(node_partiton_leader)

    return tnp


def get_all_loader_topics(admin_client: KafkaAdminClient) -> List[str]:

    # TODO: Make this method use a regex on the topic name pattern instead
    LOG.info("Fetching topic names")
    topics: Set[str] = admin_client._client.cluster.topics(exclude_internal_topics=True)

    LOG.debug("Creating Kafka Admin Client")
    admin_client.close()

    return [topic for topic in topics
            if not any([True for excluded in EXCLUDED_TOPICS if excluded in topic])]


def create_topics(
    admin_client: KafkaAdminClient,
    num_topics: int,
    partitions_per_topic: int,
    num_partition_replicas: int,
    timeout_ms: int,
    max_batch: int = 50
) -> List[Response]:

    batches: List[List[NewTopic]] = []

    LOG.info(
        "Creating %d topics each with %d partitions which are replicated %d times",
        num_topics,
        partitions_per_topic,
        num_partition_replicas,
    )

    prefix: str = dt.datetime.utcnow().strftime("%H-%M")

    LOG.info("Creating topic objects")
    topic_list: List[NewTopic] = []
    for i in range(num_topics):
        LOG.debug("Creating topic object %d", i)
        topic_list.append(
            NewTopic(
                name=f"test-topic-{prefix}_{i}",
                num_partitions=partitions_per_topic,
                replication_factor=num_partition_replicas,
            )
        )

    LOG.info("Batching topic objects")
    topic_batches: List[List[NewTopic]] = []
    topic_batch: List[NewTopic] = []
    for i, topic in enumerate(topic_list):
        if i % max_batch <= 0:
            if topic_batch:
                topic_batches.append(topic_batch)
            topic_batch = []
        topic_batch.append(topic)
    topic_batches.append(topic_batch)

    batch_creation_responses: List[Response] = []
    for i, topic_batch in enumerate(topic_batches):
        LOG.info("Submitting topic batch %d (size %d) to Kafka Admin Client for creation",
                 i, len(topic_batch))
        response: Response = admin_client.create_topics(
            new_topics=topic_batch, validate_only=False, timeout_ms=timeout_ms
        )
        batch_creation_responses.append(response)

    return batch_creation_responses


def run_topic_creation(
    bootstrap_servers: Union[List[str], str],
    num_topics: int,
    partitions_per_topic: int,
    num_partition_replicas: int,
    response_timeout: int = 60000,
) -> List[str]:

    LOG.debug("Creating Kafka Admin Client")
    admin_client: KafkaAdminClient = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="topic-creator",
        metadata_max_age_ms=response_timeout,
    )

    responses: List[Response] = create_topics(
        admin_client,
        num_topics,
        partitions_per_topic,
        num_partition_replicas,
        timeout_ms=response_timeout,
    )

    topic_list: List[str] = []
    for batch_response in responses:
        for topic_error in batch_response.topic_errors:
            if topic_error[2] is None:
                topic_list.append(topic_error[0])
            else:
                LOG.error("Error in topic %s", topic_error[0])

    LOG.debug("Closing Kafka Admin Client")
    admin_client.close()

    return topic_list
