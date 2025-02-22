from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, expr, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import json
import re
from config import *
from datetime import datetime
from transformations import *

LOG_SCHEMA = StructType([
    StructField("ip", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("method", StringType(), True),
    StructField("url", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("bytes", IntegerType(), True),
    StructField("referrer", StringType(), True),
    StructField("user_agent", StringType(), True)
])

# Regex pattern
log_pattern = re.compile(
    r'(\S+) - - \[(.*?)\] "(?:(\S+) (\S+) (\S+))?" (\d{3}) (\d+|-) "(.*?)" "(.*?)"'
)

def create_spark_session(app_name, executor_memory="1g", executor_cores="4"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .master("spark://spark:7077") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", executor_cores) \
        .config("spark.streaming.concurrentJobs", 1) \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .config("spark.cassandra.auth.username", CASSANDRA_USER) \
        .config("spark.cassandra.auth.password", CASSANDRA_PASSWORD) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.5.0") \
        .getOrCreate()


def parse_log(log):
    match = log_pattern.match(log)
    if match:
        ip, timestamp, method, url, protocol, status, bytes, referrer, user_agent = match.groups()
        
        # Handle NULL values
        method = method if method else "NULL"
        url = url if url else "NULL"
        protocol = protocol if protocol else "NULL"
        bytes = int(bytes) if bytes.isdigit() else 0
        
        # Convert timestamp to standard format
        try:
            parsed_time = datetime.strptime(timestamp.split()[0], "%d/%b/%Y:%H:%M:%S")
            formatted_timestamp = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
        except:
            formatted_timestamp = timestamp
        
        return json.dumps({
            "ip": ip,
            "timestamp": formatted_timestamp,
            "method": method,
            "url": url,
            "protocol": protocol,
            "status": int(status),
            "bytes": bytes,
            "referrer": referrer,
            "user_agent": user_agent
        })
    return None

def write_to_cassandra(table):
    def _write_to_cassandra(batch_df, batch_id):
        try:
            batch_df.write \
                .mode("append") \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=table, keyspace=CASSANDRA_KEY_SPACE) \
                .save()
            print(f"Successfully wrote batch {batch_id} to table {table}")
        except Exception as e:
            print(f"Error writing batch {batch_id} to table {table}: {str(e)}")
            raise e
    return _write_to_cassandra


def write_to_elasticsearch(batch_df, batch_id):
    try:
        print(f"Starting to write batch {batch_id} to Elasticsearch")
        
        processed_df = analyze_traffic_patterns(batch_df)
        df_with_ma = calculate_moving_averages(processed_df)
        final_df = detect_anomalies(df_with_ma)
        
        final_df = final_df \
            .withColumn("timestamp", col("timestamp").cast("timestamp")) \
            .withColumn("window_start", col("window_start").cast("timestamp")) \
            .withColumn("window_end", col("window_end").cast("timestamp"))
        
        print(f"Number of aggregated records: {final_df.count()}")
        
        final_df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", ES_NODE) \
            .option("es.port", ES_PORT) \
            .option("es.resource", "logs_index") \
            .option("es.mapping.id", "timestamp") \
            .option("es.batch.size.entries", "1000") \
            .option("es.batch.write.refresh", "wait_for") \
            .mode("append") \
            .save()
            
        print(f"Successfully wrote batch {batch_id} to Elasticsearch")
        
    except Exception as e:
        print(f"Error writing batch {batch_id} to Elasticsearch")
        print(f"Error details: {str(e)}")
        import traceback
        traceback.print_exc()

def main():
    spark = create_spark_session("RealtimeLogsProcessing", "2g", "4")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.streaming.backpressure.enabled", "true")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "10000") \
        .load()

    parse_log_udf = udf(parse_log, StringType())

    df_parsed = df.withColumn("value", df["value"].cast("string")) \
                  .withColumn("json_data", parse_log_udf(col("value"))) \
                  .withColumn("values_json", from_json(col("json_data"), LOG_SCHEMA)) \
                  .selectExpr("values_json.*") \
                  .filter(col("timestamp").isNotNull()) \
                  .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
                  .withColumn("day_bucket", to_date(col("timestamp"))) \
                  .withWatermark("timestamp", "10 minutes")
                  
    query_console = df_parsed.writeStream \
        .format("console") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
   
    # query1 = df_parsed.writeStream \
    #     .foreachBatch(write_to_cassandra("raw_logs")) \
    #     .trigger(processingTime='30 seconds') \
    #     .outputMode("append") \
    #     .option("checkpointLocation", "/tmp/checkpoints/raw_logs") \
    #     .start()
    
    query2 = df_parsed.writeStream \
        .foreachBatch(write_to_elasticsearch) \
        .trigger(processingTime='10 seconds') \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/elasticsearch") \
        .start()
        
    query2.awaitTermination()

if __name__ == "__main__":
    main()