from confluent_kafka import Consumer, KafkaException, KafkaError, Producer
import json
import logging
import os
import time

# Global variables to store device counts and previous timestamp
ios_count = {}
android_count = {}
print_interval = 1000  # Print interval in seconds
last_print_time = 0
previous_timestamp = 0
skipped_records = []  # List to store skipped records

# GDP data for states (simplified example, actual GDP data should be used)
state_gdp = {
    'AL': 'medium', 'AK': 'low', 'AZ': 'medium', 'AR': 'low', 'CA': 'high',
    'CO': 'medium', 'CT': 'high', 'DE': 'medium', 'FL': 'high', 'GA': 'high',
    'HI': 'medium', 'ID': 'low', 'IL': 'high', 'IN': 'medium', 'IA': 'medium',
    'KS': 'medium', 'KY': 'medium', 'LA': 'medium', 'ME': 'low', 'MD': 'high',
    'MA': 'high', 'MI': 'high', 'MN': 'medium', 'MS': 'low', 'MO': 'medium',
    'MT': 'low', 'NE': 'low', 'NV': 'medium', 'NH': 'medium', 'NJ': 'high',
    'NM': 'low', 'NY': 'high', 'NC': 'high', 'ND': 'low', 'OH': 'high',
    'OK': 'low', 'OR': 'medium', 'PA': 'high', 'RI': 'medium', 'SC': 'medium',
    'SD': 'low', 'TN': 'medium', 'TX': 'high', 'UT': 'medium', 'VT': 'low',
    'VA': 'high', 'WA': 'high', 'WV': 'low', 'WI': 'medium', 'WY': 'low'
}

def create_producer(bootstrap_servers):
    producer_conf = {
        'bootstrap.servers': bootstrap_servers
    }
    return Producer(producer_conf)

def produce_message(producer, topic, message):
    producer.produce(topic, value=json.dumps(message).encode('utf-8'))
    producer.flush()

def process_message(producer, output_topic, message):
    global last_print_time, previous_timestamp  # Declare as global

    try:
        data = json.loads(message)
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
            if device_type == 'iOS':
                ios_count[locale] = ios_count.get(locale, 0) + 1
            elif device_type == 'android':
                android_count[locale] = android_count.get(locale, 0) + 1

            # Processed data to be sent to new topic
            processed_data = {
                'device_type': device_type,
                'locale': locale,
                'timestamp': timestamp,
                'count': ios_count[locale] if device_type == 'iOS' else android_count[locale]
            }
            produce_message(producer, output_topic, processed_data)

            # Check if it's time to print based on print_interval
            current_time = time.time()
            if timestamp - last_print_time >= 10:
                print(f"\nTimestamp: {timestamp}")
                print("iOS devices:")
                print(json.dumps(ios_count, indent=4))
                print("\nAndroid devices:")
                print(json.dumps(android_count, indent=4))
                last_print_time = timestamp

    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def consume_messages(bootstrap_servers, input_topic, output_topic):
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

        # Final print of counts
        print("\nFinal counts:")
        print("iOS devices:")
        print(json.dumps(ios_count, indent=4))
        print("\nAndroid devices:")
        print(json.dumps(android_count, indent=4))

        # Print top 3 states for each device type and correlate with GDP
        print("\nTop 3 iOS states:")
        top_ios_states = sorted(ios_count.items(), key=lambda x: x[1], reverse=True)[:3]
        for state, count in top_ios_states:
            print(f"{state}: {count} devices (GDP: {state_gdp.get(state, 'unknown')})")

        print("\nTop 3 Android states:")
        top_android_states = sorted(android_count.items(), key=lambda x: x[1], reverse=True)[:3]
        for state, count in top_android_states:
            print(f"{state}: {count} devices (GDP: {state_gdp.get(state, 'unknown')})")

        # Print skipped records
        print("\nSkipped records:")
        for record in skipped_records:
            print(record)

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":

    # Define the bootstrap server and topics
    bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:29092')
    input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'user-login')
    output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'processed-data')

    # Start consuming messages
    consume_messages(bootstrap_servers, input_topic, output_topic)
