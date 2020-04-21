import logging

import datetime as dt

from collections import defaultdict
from typing import List, Union, Set, Dict, DefaultDict

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.protocol.admin import Response
from kafka.structs import PartitionMetadata

LOG: logging.Logger = logging.getLogger("kafka-topic-loader.topics")


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

    tnp: Dict[str, Dict[int, List[int]]] = {}

    for topic, partition_dict in partition_metadata.items():
        node_partiton_leader: DefaultDict[int, List[int]] = defaultdict(list)
        for partition, pmd in partition_dict.items():
            node_partiton_leader[pmd.leader].append(partition)
        tnp[topic] = dict(node_partiton_leader)

    return tnp


def get_all_topics(bootstrap_servers: str) -> List[str]:

    LOG.debug("Creating Kafka Admin Client")
    admin_client: KafkaAdminClient = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="topic-fetcher",
        metadata_max_age_ms=30000,
    )

    LOG.info("Fetching topic names")
    topics: Set[str] = admin_client._client.cluster.topics(exclude_internal_topics=True)

    LOG.debug("Creating Kafka Admin Client")
    admin_client.close()

    return [topic for topic in topics if "__" not in topic]


def create_topics(
    admin_client: KafkaAdminClient,
    num_topics: int,
    partitions_per_topic: int,
    num_partition_replicas: int,
    timeout_ms: int,
) -> Response:

    topic_list: List[NewTopic] = []

    LOG.info(
        "Creating %d topics each with %d partitions which are replicated %d times",
        num_topics,
        partitions_per_topic,
        num_partition_replicas,
    )

    prefix: str = dt.datetime.utcnow().strftime("%H-%M")

    for i in range(num_topics):
        LOG.debug("Creating topic %d", i)
        topic_list.append(
            NewTopic(
                name=f"test-topic-{prefix}-{i}",
                num_partitions=partitions_per_topic,
                replication_factor=num_partition_replicas,
            )
        )

    response: Response = admin_client.create_topics(
        new_topics=topic_list, validate_only=False, timeout_ms=timeout_ms
    )

    return response


def run_topic_creation(
    bootstrap_servers: Union[List[str], str],
    num_topics: int,
    partitions_per_topic: int,
    num_partition_replicas: int,
    response_timeout: int = 30000,
) -> List[str]:

    LOG.debug("Creating Kafka Admin Client")
    admin_client: KafkaAdminClient = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="topic-creator",
        metadata_max_age_ms=30000,
    )

    response: Response = create_topics(
        admin_client,
        num_topics,
        partitions_per_topic,
        num_partition_replicas,
        timeout_ms=response_timeout,
    )

    topic_list: List[str] = []
    for topic_result in response.topic_errors:
        if topic_result[2] is None:
            topic_list.append(topic_result[0])
        else:
            LOG.error("Error in topic %s", topic_result[0])

    LOG.debug("Closing Kafka Admin Client")
    admin_client.close()

    return topic_list
