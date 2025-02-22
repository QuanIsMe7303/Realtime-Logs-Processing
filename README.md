# Realtime Logs Processing Pipeline

This project is a real-time log processing pipeline that ingests log data from Kafka, processes it with Apache Spark Streaming, and writes the processed data to both Cassandra and Elasticsearch for storage and visualization. The system is containerized using Docker Compose.

The web server access logs used in this project are obtained from [Kaggle](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs?resource=download). Please refer to the dataset page for additional details and licensing information.

## Architecture Overview

The pipeline consists of the following components:

- **Log Generator**: Simulates log data by reading from a file and sending logs to a Kafka topic.
- **Kafka**: Acts as the messaging system transporting log messages.
- **Spark Streaming**: Reads log data from Kafka, applies transformations and aggregations, and writes data to:
  - **Cassandra**: Stores raw log records.
  - **Elasticsearch**: Stores aggregated metrics for real-time visualization in Kibana.
- **Cassandra**: NoSQL database used for storing raw logs.
- **Elasticsearch & Kibana**: Used for indexing, searching, and visualizing aggregated log metrics.


![Architecture Diagram](/images/architecture.png)

## Getting Started

### 1. Clone the repository
```bash
git clone https://github.com/QuanIsMe7303/Realtime-Logs-Processing.git
```
### 2. Start the environment
```
docker-compose up --build -d
```

This command starts:

- Zookeeper & Kafka for messaging.
- Cassandra for storing raw logs.
- Elasticsearch and Kibana for data visualization.
- Spark Master and Workers.
- The Spark application (pyspark-app) that processes logs.
- The Log Generator to simulate log data.

### 3. Configure Data Views in Kibana
#### 3.1. Run this command to create Index Pattern
```sh
# Create an index in Elasticsearch for storing log data 
curl -X PUT "http://elasticsearch:9200/logs_index" -H "Content-Type: application/json" -d'
{
  "mappings": {
    "properties": {
      "timestamp": {
        "type": "date",
        "format": "strict_date_optional_time||epoch_millis"
      },
      "window_start": {
        "type": "date",
        "format": "strict_date_optional_time||epoch_millis"
      },
      "window_end": {
        "type": "date",
        "format": "strict_date_optional_time||epoch_millis"
      },
      
      "request_count": { "type": "integer" },
      "avg_bytes": { "type": "float" },
      "total_bytes": { "type": "long" },
      "error_count": { "type": "integer" },
      "success_count": { "type": "integer" },
      "unique_visitors": { "type": "integer" },
      
      "path": {
        "type": "keyword",
        "fields": {
          "text": {
            "type": "text"
          }
        }
      },
      "path_hits": { "type": "integer" },
      "path_avg_bytes": { "type": "float" },
      "path_errors": { "type": "integer" },
      
      "browser_type": { "type": "keyword" },
      "browser_requests": { "type": "integer" },
      "browser_avg_bytes": { "type": "float" },
      
      "ip_class": { "type": "keyword" },
      "ip_requests": { "type": "integer" },
      "unique_ips": { "type": "integer" },
      
      "p95_response_size": { "type": "long" },
      "p99_response_size": { "type": "long" },
      "max_response_size": { "type": "long" },
      "min_response_size": { "type": "long" },
      
      "method": { "type": "keyword" },
      "method_count": { "type": "integer" },
      "method_avg_bytes": { "type": "float" },
      
      "avg_requests_ma3": { "type": "float" },
      "avg_bytes_ma3": { "type": "float" },
      "error_rate_ma3": { "type": "float" },
      
      "is_traffic_spike": { "type": "boolean" },
      "is_error_spike": { "type": "boolean" },
      "is_bandwidth_spike": { "type": "boolean" }
    }
  },
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1,
    "index": {
      "refresh_interval": "5s"
    }
  }
}'
```
#### 3.2. Create chart to visualize data
- Open Kibana at http://localhost:5601.
- Create a new Data View (Index Pattern) for the logs_index in Elasticsearch.
- Set the Time Filter field name to timestamp to use the time-based filter.
- Create dashboards and visualizations (e.g., line charts, bar charts) to monitor metrics such as request counts, average bytes, error counts, etc.

### 4. Running the Spark Application
```sh
spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.5.0 /opt/spark-apps/main.py
```
The Spark Streaming job reads logs from Kafka, applies transformations, and writes data to both Cassandra and Elasticsearch:

- **Raw Logs** are written to the Cassandra table raw_logs (with an added day_bucket column).
- **Aggregated Metrics** are written to Elasticsearch under the index logs_index.

The application uses checkpointing to ensure fault tolerance.

## Demo
### Data Ingestion into Kafka
![](/images/kafka-producer.png)

### Data Transformation and Aggregation with Spark Streaming
![](/images/spark_streaming.png)

### Storing Raw Data in Cassandra
![](/images/cassandra.png)

### Visualizing Data with Elasticsearch and Kibana
![](/images/realtime_charts.png)
