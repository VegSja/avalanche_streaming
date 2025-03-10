# Avalanche Streaming Pipeline 📊

A data pipeline that streams avalanche data from [varsom.no](https://www.varsom.no/) using a combination of Airflow, Kafka, Spark, and Cassandra. The pipeline fetches data on a daily basis, processes it, and stores it for further use, particularly for the Boreas model (see [Boreas GitHub repository](https://github.com/VegSja/Boreas)).

This project leverages Docker Compose for easy setup and deployment, while ensuring reproducibility with a provided Nix flake. 🐳

## Architecture Overview 🔄
![Pipeline Architecture](architecture.png)

The pipeline is composed of several services, each playing a key role in the data streaming process:

1. **Airflow**: Orchestrates the pipeline by fetching data daily from varsom.no.
2. **Kafka**: Acts as the message broker, streaming the data to other components.
3. **Apache Spark (PySpark)**: Processes and cleans the data.
4. **Cassandra**: Stores the cleaned data in a scalable data warehouse.
5. **Docker Compose**: Handles the orchestration of all the services.
6. **Nix Flake**: Provides a reproducible development environment.
7. **Airflow Web UI & Kafka Management**: Monitors the data pipeline.



## Getting Started 🚀

### Prerequisites

Before you get started, make sure you have the following installed on your machine:

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Nix](https://nixos.org/nix/)
- [Python 3](https://www.python.org/)

### Quick Setup

Clone this repository:

```bash
git clone https://github.com/yourusername/profile_streaming_pipeline.git
cd profile_streaming_pipeline
```
Launch the nix shell. This should install all relevant development dependencies:
```sh
nix develop
```

To launch the pipeline using Docker Compose:

```bash
docker-compose up -d
```

This will start the necessary services such as Zookeeper, Kafka, Airflow, Cassandra, etc.

### Submitting Spark jobs
To submit spark jobs run the following command:
```sh
docker compose exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf "spark.cassandra.connection.host=cassandra_db" \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.commons:commons-pool2:2.8.0,org.apache.kafka:kafka-clients:2.5.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.3.2 \
  /opt/spark/spark-jobs/avalanche_processor.py
```
### Monitoring and Management 🖥️

- **Airflow UI**: Access the web server at [http://localhost:8080](http://localhost:8080) to monitor and manage the pipeline.
- **Kafka UI**: Use [Confluent Control Center](http://localhost:9021) to manage Kafka topics and streams.
- **Cassandra UI**: Use the built-in Cassandra web interface to monitor the database.

## Services Overview 🔧

### 🦄 **Zookeeper**
Zookeeper is responsible for coordinating distributed services, specifically Kafka.

- **Ports**: 2181
- **Healthcheck**: Ensures Zookeeper is running correctly.

### 📡 **Kafka Broker**
Kafka is the message broker that streams the avalanche data.

- **Ports**: 9092, 9101

### 📜 **Schema Registry**
Manages the schemas for the data being streamed by Kafka.

- **Ports**: 8081

### 🖥️ **Control Center**
Confluent's Control Center is a GUI to manage Kafka, check metrics, and configure topics.

- **Ports**: 9021

### 🌐 **Airflow Webserver**
The Airflow webserver allows you to monitor the status of DAGs (Directed Acyclic Graphs), which orchestrate the data fetching and processing.

- **Ports**: 8080

### 📅 **Airflow Scheduler**
The scheduler executes the tasks defined in the Airflow DAGs.

### 🗄️ **PostgreSQL**
PostgreSQL is used as the metadata store for Airflow.

- **Ports**: 5432

### 🛠 **Apache Spark (PySpark)**
Apache Spark (via PySpark) is used to process and clean the incoming avalanche data.

- **Image**: Custom Docker image (not defined in the Docker Compose here)
- **Responsibilities**: Data processing and formatting

### 💾 **Cassandra**
Cassandra is used to store the processed data in a distributed manner.

## Contributing 🤝

If you'd like to contribute to this project:

1. Fork this repository.
2. Create a feature branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Create a new Pull Request.

