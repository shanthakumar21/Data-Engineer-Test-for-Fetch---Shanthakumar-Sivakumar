<h1>Data Engineer Test for Fetch Rewards</h1>



This repository contains the code for a data engineering test project for Fetch Rewards. The project involves a real-time streaming data pipeline using Kafka and Docker, with a Flask server with dash and other components to display the data on a web dashboard.


<h2>Folder Structure<h2>

![Folder structure](https://github.com/user-attachments/assets/eee87450-eb7b-473b-bfb4-27d8a49d0746)



<h2>Getting Started</h2>


<h3>System Flow Diagram</h3>

![Flowdiagram](https://github.com/user-attachments/assets/3e9babf4-4719-43c1-ba6a-18ac577dc668)

This project involves building a real-time data processing and analytics pipeline using Kafka, Docker, Python, and Dash. It ingests device data through a Kafka producer, which is then consumed by a Kafka consumer written in Python. The consumer processes messages, categorizing devices by type (iOS, Android, others) and by US state, and updates global state device counts and also converts the timestamps to readable formats and checks for missing data points. These counts are then exposed via a Flask server, which provides RESTful endpoints for real-time data and insights. The Flask server also integrates with Dash to create a live-update dashboard, displaying interactive graphs and insights about device distributions across states. Docker is used to containerize and orchestrate all the components, ensuring seamless deployment and scalability. The project is set up and run using Docker Compose, making it easy to manage all the interconnected services.


<h3>Video Demonstration</h3>

https://github.com/user-attachments/assets/755dd4ff-f55d-4987-86c2-96212fcf192e



<h3>Prerequisites</h3>

Ensure you have the following installed on your machine:

->Docker 

->Docker Compose



<h3>Cloning the Repository</h3>

Clone this repository to your local machine by executing the following command on you CMD:

```bash
$ git clone https://github.com/shanthakumar21/data-engineer-test-for-fetch.git

$ cd data-engineer-test-for-fetch
```


<h3>Running the Project</h3>

To build and run the Docker containers, execute the following command:

```bash
docker-compose up --build
```

This will start all necessary services including Zookeeper, Kafka, the kafka producer, the Kafka consumer, and the Flask server.



<h3>Accessing the Application</h3>

Once the containers are up and running, you can access the Flask server by navigating to:

```bash
http://localhost:5000
```

Here, you should be able to see the real-time data and insights provided by the Flask server.




<h2>Project Components</h2>



<h3>Kafka Consumer</h3>

The Kafka consumer is located in src/consumer/consumer.py. It plays a crucial role in the data pipeline by reading messages from the Kafka topic and processing them. 


<h3>Design Features of Consumer.py</h3>

<h3>Configuration:</h3>

->Connects to the Kafka broker using bootstrap servers specified in the environment variables.

->Subscribes to the desired topic. 

<h3>Message Processing:</h3>

->Continuously polls the Kafka topic for new messages.

->Converts unread-able timestamps to readable timestamps.

->Decodes and processes each message, updating the global state for device counts.

->Categorizes devices into iOS, Android, or others, and maps them to US states.


<h3>Concurrency:</h3>

->Runs in a separate thread to ensure non-blocking operation and continuous message consumption.


<h3>Error Handling:</h3>

->Handles various Kafka errors gracefully to ensure robustness.


<h3>Flask Server</h3>

->The Flask server is located in src/web/Server.py. It serves as the backend for the web application and provides several key functionalities:


<h3>Endpoints:</h3>

->Home Page (/): Serves the main dashboard where the data and insights are displayed using HTML templates.

->Data Endpoint (/data): Provides a JSON response with real-time data about device counts categorized by US states.

->Insights Endpoint (/insights): Offers insights based on the processed data, such as the state with the highest total devices, states with more iOS or Android devices, and comparisons between device types and GDP classifications of states.


<h3>Data Processing:</h3>

->The server processes messages from Kafka, updating device counts in real-time.

->It maintains a global state to keep track of device counts and time-series data for iOS and Android devices.


<h3>Real-time Dashboard:</h3>

->Utilizes Dash to create interactive graphs and insights displayed on the web application.

->A live-update graph shows the count of iOS and Android devices over time.

->Insights are updated regularly based on new data from Kafka.


<h3>HTML Templates:</h3>

->Uses HTML with Flask to render dynamic web pages.

->The templates directory contains the HTML files used for rendering the web pages.


<h3>Concurrency:</h3>

->Runs a separate thread for consuming messages from Kafka to ensure that the Flask server remains responsive.


<h3>Docker</h3>

The Dockerfiles for the consumer and the Flask server are located in src/consumer/Dockerfile.consumer and src/web/Dockerfile.server respectively. The docker-compose.yml file orchestrates the entire setup.




<h3>Notes</h3>

->Ensure the Kafka consumer starts before the Flask server to allow data to be available when the server starts. The wait time is set to 50s for now. Update it in main function of the server.py file. 

->The project is configured to restart the consumer on failure up to 10 times.

->Use docker-compose logs -f to monitor the logs and ensure all services are running correctly.
