import logging

from typing import List
from argparse import ArgumentParser, Namespace

from kafka.errors import NoBrokersAvailable

from topics import run_topic_creation, get_all_topics
from producers import send_messages

LOG: logging.Logger = logging.getLogger("kafka-topic-loader.cli")


def create_parser() -> ArgumentParser:

    parser: ArgumentParser = ArgumentParser()

    parser.add_argument(
        "bootstrap_servers", help="Bootstrap server address [host]:[port]",
    )

    parser.add_argument(
        "--debug",
        required=False,
        action="store_true",
        help="Enable debug level logging",
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

    producer_parser: ArgumentParser = subparsers.add_parser(
        "producer",
        prog="Load Kafka topics with messages",
        help="Performs the message loading into the Kafka topics",
    )

    producer_parser.add_argument(
        "-int",
        "--interval",
        type=float,
        required=False,
        default=0.1,
        help="Time interval in seconds between sending blocks",
    )

    producer_parser.set_defaults(func=run_producer)

    return parser


def create_main_logger(debug: bool) -> logging.Logger:

    main_log: logging.Logger = logging.getLogger("kafka-topic-loader")

    if debug:
        level = logging.DEBUG
        fmt = (
            "{levelname} | {name} | "
            "function: {funcName} "
            "| line: {lineno} | {message}"
        )

        style = "{"
    else:
        level = logging.INFO
        fmt = "{asctime} | {name} | {levelname} " "| {message}"
        style = "{"

    console: logging.StreamHandler = logging.StreamHandler()
    console.setFormatter(logging.Formatter(fmt=fmt, style=style))

    main_log.setLevel(level)
    main_log.addHandler(console)

    return main_log


def run_topics_creation(args) -> None:

    LOG.info("Running topic creation process...")

    try:
        run_topic_creation(
            args.bootstrap_servers,
            args.num_topics,
            args.partitions_per_topic,
            args.num_partition_replicas,
        )
    except NoBrokersAvailable:
        LOG.error("No brokers available at address: %s", args.bootstrap_servers)


def run_producer(args) -> None:

    topics: List[str] = get_all_topics(args.bootstrap_servers)

    LOG.info("Sending messages to %d topics on the Kafka Cluster", len(topics))
    send_messages(args.bootstrap_servers, topics, args.interval)


if __name__ == "__main__":

    PARSER: ArgumentParser = create_parser()
    ARGS: Namespace = PARSER.parse_args()

    MAIN_LOG: logging.Logger = create_main_logger(ARGS.debug)

    try:
        ARGS.func(ARGS)
    except AttributeError:
        PARSER.error("Too few arguments, expected sub-command")
