import os
import json
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
from flask import Flask, render_template, jsonify

# Dictionary mapping US state abbreviations to GDP classifications
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

# Global variable to store state device counts
state_device_counts = defaultdict(lambda: {'ios': 0, 'android': 0, 'others': 0, 'gdp': 'unknown', 'total': 0})

# Initialize Flask app
app = Flask(__name__)

# Route for home page
@app.route('/')
def index():
    return render_template('index.html')

# Route to get the state device counts
@app.route('/data')
def get_data():
    return jsonify(state_device_counts)

def consume_messages(consumer, output_topic):
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
                process_message(msg.value().decode("utf-8"))

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer
        consumer.close()

def process_message(message):
    global state_device_counts

    try:
        data = json.loads(message)
        location = data.get('location')
        device_type = data.get('device_type')

        if location in state_gdp:
            state_device_counts[location]['gdp'] = state_gdp[location]
            if device_type == 'iOS':
                state_device_counts[location]['ios'] += 1
            elif device_type == 'android':
                state_device_counts[location]['android'] += 1
            else:
                state_device_counts[location]['others'] += 1

            state_device_counts[location]['total'] = (state_device_counts[location]['ios'] + 
                                                      state_device_counts[location]['android'] + 
                                                      state_device_counts[location]['others'])

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

if __name__ == "__main__":
    # Kafka consumer configuration
    bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:29092')
    input_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'processed-data')

    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest'
    }

    # Create Consumer instance
    consumer = Consumer(consumer_conf)

    # Subscribe to the topic
    consumer.subscribe([input_topic])

    # Start consuming messages in a separate thread
    import threading
    threading.Thread(target=consume_messages, args=(consumer, input_topic), daemon=True).start()

    # Start Flask app
    app.run(debug=True)
