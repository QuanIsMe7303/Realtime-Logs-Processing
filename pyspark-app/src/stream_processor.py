from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, expr, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import json
import re
from config import *
from datetime import datetime

LOG_SCHEMA = StructType([
    StructField("ip", StringType(), True),
    StructField("timestamp", StringType(), True),
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

def write_to_cassandra(batch_df, batch_id):
    """Ghi dữ liệu batch vào Cassandra"""
    print("Batch id: "+ str(batch_id))
    # Thêm cột day_bucket để partitioning
    batch_df = batch_df.withColumn("timestamp", col("timestamp").cast(TimestampType())) \
                      .withColumn("day_bucket", to_date(col("timestamp")))
    
    # Ghi vào Cassandra
    batch_df.write \
        .mode("append") \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="raw_logs", keyspace=CASSANDRA_KEY_SPACE) \
        .save()
    
parse_log_udf = udf(parse_log, StringType())

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("spark://spark:7077") \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .config("spark.cassandra.auth.username", CASSANDRA_USER) \
    .config("spark.cassandra.auth.password", CASSANDRA_PASSWORD) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                                   "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df = df.withColumn("value", df["value"].cast("string"))

# Chuyển đổi log thành JSON
df_parsed = df.withColumn("json_data", parse_log_udf(col("value")))

# Chuyển thành DataFrame với schema cụ thể
streaming_df = df_parsed.withColumn("values_json", from_json(col("json_data"), LOG_SCHEMA)) \
                        .selectExpr("values_json.*")

# Ghi vào console để debug
# console_query = streaming_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .trigger(processingTime='10 seconds') \
#     .option("truncate", "true") \
#     .option("checkpointLocation", "/opt/spark/checkpoints/console") \
#     .start()

# Ghi vào Cassandra
streaming_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", "/opt/spark/checkpoints/cassandra") \
    .start() \
    .awaitTermination()

# Chờ cả hai queries
# spark.streams.awaitAnyTermination()