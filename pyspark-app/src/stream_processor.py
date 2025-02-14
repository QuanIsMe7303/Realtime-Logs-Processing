from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json
import re
from config import *

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
        
        # Xử lý NULL nếu không có method, url, protocol (trường hợp lỗi parsing)
        method = method if method else "NULL"
        url = url if url else "NULL"
        protocol = protocol if protocol else "NULL"
        bytes = int(bytes) if bytes.isdigit() else 0  # Nếu `-` thì set về 0
        
        return json.dumps({
            "ip": ip,
            "timestamp": timestamp,
            "method": method,
            "url": url,
            "protocol": protocol,
            "status": int(status),
            "bytes": bytes,
            "referrer": referrer,
            "user_agent": user_agent
        })
    return None

parse_log_udf = udf(parse_log, StringType())

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("spark://spark:7077") \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

df = df.withColumn("value", df["value"].cast("string"))

# Chuyển đổi log thành JSON
df_parsed = df.withColumn("json_data", parse_log_udf(col("value")))

# Chuyển thành DataFrame với schema cụ thể
streaming_df = df_parsed.withColumn("values_json", from_json(col("json_data"), LOG_SCHEMA)) \
                        .selectExpr("values_json.*")

# Hiển thị dữ liệu trên console
query = streaming_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "true") \
    .start()

query.awaitTermination()
