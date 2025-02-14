from pyspark.sql import SparkSession

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
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "logs-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Xử lý dữ liệu
df = df.selectExpr("CAST(value AS STRING)")

# Hiển thị dữ liệu trên console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
