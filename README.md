# Weather_streaming

Weather Streaming Project

This project demonstrates a real-time weather data pipeline using the OpenWeather API, Kafka, Apache Spark, Cassandra, and Grafana. The pipeline collects random weather data from Tunisian states, processes it in real-time, stores it in Cassandra, and visualizes it using Grafana.
Project Overview
Technologies Used

  OpenWeather API: For fetching weather data.
  Kafka: For real-time data streaming.
  Apache Spark: For processing streaming data.
  Cassandra: For scalable, distributed data storage.
  Grafana: For data visualization.

Pipeline Workflow

  Producer: Fetches weather data from the OpenWeather API and sends it to a Kafka topic.
  Stream Processing: Spark processes the streaming data from Kafka and stores it in Cassandra.
  Storage & Export: Data stored in Cassandra is exported to a CSV file for further cleaning.
  Visualization: Cleaned data is visualized in Grafana.

Project Setup
1. Initial Setup

    Create a new project folder and navigate to it:

mkdir spark_project
cd spark_project

Create and activate a Python virtual environment:

    python -m venv myenv
    source myenv/bin/activate

2. Start Docker Containers

    Use the docker-compose.yml file to pull and run necessary containers:

       sudo docker-compose up -d

Running the Pipeline
1. Producer Script

Run the script to fetch weather data and send it to Kafka:

    python producer_weather.py

2. Install Required Spark Packages

Install necessary packages for Spark to connect with Kafka and Cassandra:

# Add packages from Maven repository and run spark_stream.py:
     spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.4,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 spark_stream.py

Data Storage and Access
1. Access Cassandra

Run Cassandra's interactive shell:

    sudo docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042

2. Query Weather Data

Check the captured events stored in Cassandra:

     SELECT * FROM spark_streams.weather;

3. Export Data to CSV

Export the Cassandra table to a CSV file:

    COPY spark_streams.weather(columns) TO 'tunisia_weather.csv' WITH HEADER = TRUE;

Data Transformation

  Export the CSV file from the Cassandra container to your local project folder:

    docker cp cassandra:/tunisia_weather.csv ~/your_path

  Run the transform_data.ipynb Jupyter Notebook to clean the data and generate clean_data.csv.

Visualization with Grafana

  Copy the cleaned CSV file to the Grafana container:

    docker cp ~/your_path/clean_data.csv grafana:/etc/

    Start Grafana and log in:
        Default credentials:
            Username: grafana
            Password: grafana

  Install the CSV Data Source extension.

  Configure the data source by providing the path to the clean_data.csv file.

  Create and customize dashboards to visualize the cleaned weather data.

Outputs

  Raw Data: Stored in Cassandra.
  
  Cleaned Data: Available as clean_data.csv from Transform_data.ipynb for visualization.
  
  Dashboards: Grafana displays real-time and historical weather data from Tunisian states.

Conclusion

This project highlights the integration of real-time data streaming, distributed storage, and visualization tools to create a scalable and insightful pipeline for weather data.

Feel free to explore the repository and reach out with any questions or suggestions!
