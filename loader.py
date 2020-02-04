from argparse import ArgumentParser, Namespace

from topics import run_topic_creation


def create_parser() -> ArgumentParser:

    parser: ArgumentParser = ArgumentParser()

    parser.add_argument(
        "-b",
        "--bootstrap_servers",
        required=True,
        help="Bootstrap server address [host]:[port]",
    )

    subparsers = parser.add_subparsers()

    topics_parser: ArgumentParser = subparsers.add_parser(
        "topics",
        prog="Kafka Topic Creation",
        help="Creates the test topics ready for loading",
    )

    topics_parser.add_argument(
        "-nt",
        "--num_topics",
        type=int,
        required=True,
        help="The number of test topics to create",
    )

    topics_parser.add_argument(
        "-ppt",
        "--partitions_per_topic",
        type=int,
        required=True,
        help="The number of partitions per topic",
    )

    topics_parser.add_argument(
        "-npr",
        "--num_partition_replicas",
        type=int,
        required=True,
        help="The number of replicas of each partition",
    )

    topics_parser.set_defaults(func=run_topics_creation)

    return parser


def run_topics_creation(args):

    run_topic_creation(
        args.bootstrap_servers,
        args.num_topics,
        args.partitions_per_topic,
        args.num_partition_replicas,
    )


if __name__ == "__main__":

    PARSER: ArgumentParser = create_parser()
    ARGS: Namespace = PARSER.parse_args()

    ARGS.func(ARGS)
