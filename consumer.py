from confluent_kafka import Consumer, KafkaException, KafkaError, Producer
import json
import logging
import os
import time
from datetime import datetime
import signal

# Global variables to store processed data, device counts, and skipped records
processed_data_list = []  # List to store processed data
skipped_records = []  # List to store skipped records
previous_timestamp = 0  # Initialize previous_timestamp
ios_count = {}
android_count = {}

def create_producer(bootstrap_servers):
    producer_conf = {
        'bootstrap.servers': bootstrap_servers
    }
    return Producer(producer_conf)

def produce_message(producer, topic, message):
    producer.produce(topic, value=json.dumps(message).encode('utf-8'))
    producer.flush()

def process_message(producer, output_topic, message):
    global previous_timestamp, ios_count, android_count  # Declare as global

    try:
        data = json.loads(message)
        user_id = data.get('user_id')
        ip = data.get('ip')
        device_type = data.get('device_type')
        locale = data.get('locale')
        timestamp = data.get('timestamp')

        # Check for out-of-place records
        if timestamp < previous_timestamp:
            logging.warning(f"Skipping out-of-place record: {data}")
            skipped_records.append({'timestamp': timestamp, 'device_type': device_type})  # Store the skipped record
            return

        previous_timestamp = timestamp

        if device_type and locale and timestamp:
            # Update device counts dynamically
            if device_type == 'iOS':
                ios_count[locale] = ios_count.get(locale, 0) + 1
            elif device_type == 'android':
                android_count[locale] = android_count.get(locale, 0) + 1

            # Processed data to be sent to new topic
            processed_data = {
                'user_id': user_id,
                'ip': ip,
                'device_type': device_type,
                'location': locale,
                'timestamp': timestamp,
                'readable_timestamp': datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            }

            produce_message(producer, output_topic, processed_data)

            # Add processed data to list
            processed_data_list.append(processed_data)

            # Display the processed data
            print(json.dumps(processed_data, indent=4))

    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def consume_messages(bootstrap_servers, input_topic, output_topic):
    global ios_count, android_count  # Declare as global

    # Consumer configuration
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest'
    }

    # Create Consumer instance
    consumer = Consumer(consumer_conf)
    producer = create_producer(bootstrap_servers)
    bootstrap_server_url = consumer_conf['bootstrap.servers']

    print(f"Bootstrap Server URL: {bootstrap_server_url}")

    # Subscribe to the topic
    consumer.subscribe([input_topic])
    
    def signal_handler(sig, frame):
        print('Stopping consumer...')
        consumer.close()
        print("\nFinal counts:")
        print("Processed Data:")
        print(json.dumps(processed_data_list, indent=4))
        print("\nSkipped Records:")
        print(json.dumps(skipped_records, indent=4))
        exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for new messages

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Process the message
                process_message(producer, output_topic, msg.value().decode("utf-8"))

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer
        consumer.close()
        print("\nFinal counts:")
        print("Processed Data:")
        print(json.dumps(processed_data_list, indent=4))
        print("\nSkipped Records:")
        print(json.dumps(skipped_records, indent=4))

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    # Define the bootstrap server and topics
    bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:29092')
    input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'user-login')
    output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'processed-data')

    # Start consuming messages
    consume_messages(bootstrap_servers, input_topic, output_topic)
