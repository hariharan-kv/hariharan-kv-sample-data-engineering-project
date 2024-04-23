#!/bin/bash

# Open terminal and start ZooKeeper
gnome-terminal -- bash -c 'cd /home/hariharan-kv/Documents/kafka_2.13-3.7.0; bin/zookeeper-server-start.sh config/zookeeper.properties; exec bash'

sleep 5  # Wait for ZooKeeper to start before proceeding

# Open terminal and start Kafka broker
gnome-terminal -- bash -c 'cd /home/hariharan-kv/Documents/kafka_2.13-3.7.0; bin/kafka-server-start.sh config/server.properties; exec bash'

sleep 5  # Wait for Kafka broker to start before proceeding

# Open terminal for Kafka topic operations
gnome-terminal -- bash -c 'cd /home/hariharan-kv/Documents/kafka_2.13-3.7.0;
bin/kafka-topics.sh --create --topic ad_impressions_topic --bootstrap-server localhost:9092 &&
bin/kafka-topics.sh --create --topic clicks_conversions_topic --bootstrap-server localhost:9092 &&
bin/kafka-topics.sh --create --topic bid_requests_topic --bootstrap-server localhost:9092; exec bash'