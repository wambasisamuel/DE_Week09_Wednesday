from confluent_kafka import Consumer, KafkaError
import json


# Function to check if topic exists
def topic_exists(client,topic_name):
    topic_metadata = client.list_topics()
    topics = [t.topic for t in iter(topic_metadata.topics.values())]
    if topic_name in topics:
        return True
    return False

# Function to describe topic: get total messages
def describe_topic(client,topic_name):
    resource = ConfigResource('topic', topic_name)
    result_dict = client.describe_configs([resource])
    config_entries = result_dict[resource].result()
    return config_entries

# Function to consume from the kafka topic
def consume_from_topic(consumer,topic):
    if not topic_exists:
        print("The topic '{}' does not exist!".format(topic))
        return
    consumer.subscribe(['{}'.format(topic)])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("End of Partition reached.")
            else:
                print("Error while consuming message: ", msg.error())
        else:
            print("Message Received: ", msg.value().decode("utf-8"))
    return


if __name__ == '__main__':
    # Topic name
    pipeline_topic = 'pipeline'

    # Set up Kafka consumer to read from "pipeline" topic
    consumer_config = {
            'bootstrap.servers':'broker:29092',
            'group.id': 'pipeline_1',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
    }
    
    consumer = Consumer(consumer_config)

    print(consumer.list_topics().topics)

    # Consume from the kafka topic
    consume_from_topic(consumer,pipeline_topic)
