from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
import json


# Function to check if topic exists
def topic_exists(client,topic_name):
    topic_metadata = client.list_topics()
    topics = [t.topic for t in iter(topic_metadata.topics.values())]
    if topic_name in topics:
        return True
    return False

# Function to create new topic
def create_topic(client,topic_name):
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    result_dict = client.create_topics([new_topic])
    for k,v in result_dict.items():
        try:
            v.result()
            print("Topic {} created".format(topic_name))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic_name, e))

# Function to describe topic: get total messages
def describe_topic(client,topic_name):
    resource = ConfigResource('topic', topic_name)
    result_dict = client.describe_configs([resource])
    config_entries = result_dict[resource].result()
    return config_entries

# Function to produce contents of json file to the kafka topic
def produce_to_topic(producer,topic,json_file):
    # json_file --> absolute path to file
    try:
        with open(json_file) as j_file:
            for line in j_file.readlines():
                cdr = json.dumps(line).encode('utf-8')
                producer.produce(topic,value=cdr)
        producer.flush()
        print("File content sent to topic successfully.")
    except Exception as e:
        print("Error while sending data to Kafka", e)

if __name__ == '__main__':
    # Create Admin client
    admin_client = AdminClient({
            "bootstrap.servers": "broker:29092"
    })
    
    # Topic name
    pipeline_topic = 'pipeline'

    # Create topic if it doesn't exist
    if not topic_exists(admin_client, pipeline_topic):
        create_topic(admin_client, pipeline_topic)
    
    # Set up Kafka producer to write to "pipeline" topic
    producer = Producer({'bootstrap.servers': 'broker:29092'})
    
    topic_metadata = admin_client.list_topics()
    
    topics = [t.topic for t in iter(topic_metadata.topics.values())]
    print(topics)
    
    input_file = "/project/input.json"

    # Produce to topic
    produce_to_topic(producer,pipeline_topic,input_file)
