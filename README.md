# Kafka Topic Loader

This script will create a user defined number of topics and replicas and then
will preferentially load certain nodes (by selecting partitions whose leader is
the chosen node) with messages. This should create an _unbalanced_ cluster for
testing the performance of the [Cruise
Control](https://github.com/linkedin/cruise-control) Kafka cluster balancing
service.

## Installation 

Dependencies are managed via [poetry](https://python-poetry.org/). To install dependencies run:

```bash
$ poetry install
```

## Using the script

### Creating topics 

To create the test topics run the following command:

```bash
$ poetry run python loader.py topics <kafka-bootstrap-addresses> topics \\
    -pt <Number of topics to be created> \\
    -ppt <number of partitions per topic> \\
    -npr <number of replicas per partition>
```

For example to create 100 topics with 10 partitions each which are replicated
3 times:

```bash 
$ poetry run python loader.py localhost:9094 topics -pt 100 -ppt 10 -npr 3
```

### Loading the topics

To load the nodes of the Kafka cluster run the `producer` sub-command:

```bash
$ poetry run python loader.py producer localhost:9094 -i 0.1
```

The `-i/--interval` argument specifies the pause (in seconds) between sending
messages, so an interval of 0.1 seconds equates to a rate of 10 messages
a second. 

To speed up the loading, invoke the script in multiple terminals.
