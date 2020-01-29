import datetime as dt

from collections import defaultdict
from typing import List, Union, Set, Dict, DefaultDict

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.protocol.admin import Response
from kafka.structs import PartitionMetadata


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

    partition_metadata: PartitionMetadata = get_partition_metadata(admin_client)

    tnp: Dict[str, Dict[int, List[int]]] = {}

    for topic, partition_dict in partition_metadata.items():
        node_partiton_leader: DefaultDict[int, List[int]] = defaultdict(list)
        for partition, pmd in partition_dict.items():
            node_partiton_leader[pmd.leader].append(partition)
        tnp[topic] = dict(node_partiton_leader)

    return tnp


def get_all_topics(admin_client: KafkaAdminClient) -> List[str]:

    topics: Set[str] = admin_client._client.cluster.topics(exclude_internal_topics=True)

    return [topic for topic in topics if "__" not in topic]


def create_topics(
    admin_client: KafkaAdminClient,
    num_topics: int,
    partitions_per_topic: int,
    num_partition_replicas: int,
    timeout_ms: int,
) -> Response:

    topic_list: List[NewTopic] = []

    print(
        f"Creating {num_topics} topics each with {partitions_per_topic} partitions "
        f"which are replicated {num_partition_replicas} times"
    )

    prefix: str = dt.datetime.utcnow().strftime("%H-%M")

    for i in range(num_topics):
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
) -> List[str]:

    print("Creating Admin Client")
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
        timeout_ms=10000,
    )

    topic_list: List[str] = []
    for topic_result in response.topic_errors:
        if topic_result[2] is None:
            topic_list.append(topic_result[0])
        else:
            print(f"Error in topic {topic_result[0]}")

    print("Closing admin client")
    admin_client.close()

    return topic_list
