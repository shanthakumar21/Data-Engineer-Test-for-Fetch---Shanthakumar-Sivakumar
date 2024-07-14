import os
import json
import requests
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
from flask import Flask, render_template, jsonify
from threading import Thread
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import time

# Dictionary mapping US state abbreviations to GDP classifications, further used to compare device type with GDP of a state
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
state_device_counts = defaultdict(lambda: defaultdict(int))
time_series_data = []

# Initialize Flask app
app = Flask(__name__)

# Initialize Dash app
dash_app = Dash(__name__, server=app, url_base_pathname='/dash/')

# Route for home page
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data')
def data():
    return jsonify({state: {'gdp': state_gdp[state],
                            'ios': counts['ios'],
                            'android': counts['android'],
                            'others': counts['others'],
                            'total': counts['ios'] + counts['android'] + counts['others']}
                    for state, counts in state_device_counts.items()})

@app.route('/insights')
# This function employs simple logic which continuously updates insights for the data points that are being fed
def insights():
    global state_device_counts

    insights = []

    # Sort states by total device counts
    sorted_states = sorted(state_device_counts.items(), key=lambda item: sum(item[1].values()), reverse=True)

    if sorted_states:
        top_state = sorted_states[0][0]
        top_total_devices = sum(sorted_states[0][1].values())
        insights.append(f"The state with the highest total devices is {top_state} with {top_total_devices} devices.")

        ios_states = [state for state, counts in sorted_states if counts['ios'] > 0]
        android_states = [state for state, counts in sorted_states if counts['android'] > 0]

        if ios_states:
            insights.append(f"States with iOS devices: {', '.join(ios_states)}")
        if android_states:
            insights.append(f"States with Android devices: {', '.join(android_states)}")

        high_gdp_ios_preference = [state for state, counts in sorted_states if state_gdp[state] == 'high' and counts['ios'] > counts['android']]
        if high_gdp_ios_preference:
            insights.append(f"States like {', '.join(high_gdp_ios_preference)} have a high GDP and a higher number of iOS devices.")

        low_gdp_android_preference = [state for state, counts in sorted_states if state_gdp[state] == 'low' and counts['android'] > counts['ios']]
        if low_gdp_android_preference:
            insights.append(f"States like {', '.join(low_gdp_android_preference)} have a low GDP and as one would assume also has a higher number of Android devices.")

    return jsonify(insights)

def consume_messages(consumer, output_topic):
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                process_message(msg.value().decode("utf-8"))

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def process_message(message):
    global state_device_counts, time_series_data

    try:
        data = json.loads(message) #collecting only necessary data for further steps. Static data and data deemed useless like ip are not collected
        location = data.get('location') 
        device_type = data.get('device_type')

        if location in state_gdp: 
            if device_type == 'iOS':
                state_device_counts[location]['ios'] += 1
            elif device_type == 'android':
                state_device_counts[location]['android'] += 1
            else:
                state_device_counts[location]['others'] += 1

            # Update time series data
            timestamp = pd.Timestamp.now()
            ios_count = sum(counts['ios'] for counts in state_device_counts.values())
            android_count = sum(counts['android'] for counts in state_device_counts.values())
            time_series_data.append({'timestamp': timestamp, 'ios': ios_count, 'android': android_count})

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

def kafka_consumer_thread():
    # Kafka consumer configuration
    bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', 'localhost:29092')
    input_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'processed-data')

    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest'
    }

    # Creating Consumer instance
    consumer = Consumer(consumer_conf)

    # Subscribe to the topic
    consumer.subscribe([input_topic])

    try:
        # Start consuming messages
        consume_messages(consumer, input_topic)
    except Exception as ex:
        print(f"Exception: {ex}")
    finally:
        consumer.close()

# Dash layout
dash_app.layout = html.Div([
    html.H1("iOS vs Android Devices Over Time"),
    dcc.Graph(id='live-update-graph'),
    html.Div(id='insights', style={'marginTop': 20}),
    dcc.Interval(
        id='interval-component',
        interval=1*1000,  # in milliseconds
        n_intervals=0
    )
])

# Dash callback for updating the graph
@dash_app.callback(
    Output('live-update-graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_graph_live(n):
    global time_series_data

    df = pd.DataFrame(time_series_data)
    if not df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['ios'], mode='lines', name='iOS Devices'))
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['android'], mode='lines', name='Android Devices'))
        fig.update_layout(template='plotly_dark')
        return fig
    return go.Figure()

# Dash callback for updating insights
@dash_app.callback(
    Output('insights', 'children'),
    [Input('interval-component', 'n_intervals')]
)
def update_insights(n):
    response = requests.get('http://0.0.0.0:5000/insights') #using 0.0.0.0 to access the webpage outside the containers localhost
    insights = response.json()

    return html.Div([
        html.H2("Insights"),
        html.Ul([html.Li(insight) for insight in insights])
    ])

if __name__ == "__main__":
    # Wait for Kafka consumer to start
    time.sleep(50)  # Adjust the sleep time as needed

    # Start Kafka consumer thread
    consumer_thread = Thread(target=kafka_consumer_thread)
    consumer_thread.start()

    # Run Flask app
    app.run(debug=True, use_reloader=False, host='0.0.0.0')
