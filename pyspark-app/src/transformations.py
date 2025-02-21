from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    window, count, avg, sum, col, hour, minute,
    explode, split, regexp_extract, when, desc,
    rank, max, min, percentile_approx, collect_set,
    from_unixtime, unix_timestamp, to_timestamp,
    array_distinct, size, lit, concat, countDistinct
)
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType

def analyze_traffic_patterns(df: DataFrame) -> DataFrame:
    # 1. Time-based metrics with 5-minute windows
    time_metrics = df.groupBy(
        window(col("timestamp"), "5 minutes")
    ).agg(
        count("*").alias("request_count"),
        avg("bytes").alias("avg_bytes"),
        sum("bytes").alias("total_bytes"),
        sum(when(col("status") >= 400, 1).otherwise(0)).alias("error_count"),
        sum(when(col("status") < 300, 1).otherwise(0)).alias("success_count"),
        countDistinct("ip").alias("unique_visitors"),
        max("timestamp").alias("timestamp")
    )

    # 2. URL Path Analysis
    path_metrics = df.withColumn(
        "path", regexp_extract(col("url"), "^/([^?#]*)", 1)
    ).groupBy(
        window(col("timestamp"), "5 minutes"),
        "path"
    ).agg(
        count("*").alias("path_hits"),
        avg("bytes").alias("path_avg_bytes"),
        sum(when(col("status") >= 400, 1).otherwise(0)).alias("path_errors")
    )

    # 3. User Agent Analysis
    browser_metrics = df.withColumn(
        "browser_type",
        when(col("user_agent").like("%Chrome%"), "Chrome")
        .when(col("user_agent").like("%Firefox%"), "Firefox")
        .when(col("user_agent").like("%Safari%"), "Safari")
        .when(col("user_agent").like("%bot%"), "Bot")
        .otherwise("Other")
    ).groupBy(
        window(col("timestamp"), "5 minutes"),
        "browser_type"
    ).agg(
        count("*").alias("browser_requests"),
        avg("bytes").alias("browser_avg_bytes")
    )

    # 4. Geographic Analysis (based on IP - simplified)
    geo_metrics = df.withColumn(
        "ip_class", 
        when(col("ip").like("10.%"), "Internal")
        .when(col("ip").like("192.168.%"), "Internal")
        .when(col("ip").like("172.16.%"), "Internal")
        .otherwise("External")
    ).groupBy(
        window(col("timestamp"), "5 minutes"),
        "ip_class"
    ).agg(
        count("*").alias("ip_requests"),
        countDistinct("ip").alias("unique_ips")
    )

    # 5. Performance Metrics
    perf_metrics = df.groupBy(
        window(col("timestamp"), "5 minutes")
    ).agg(
        percentile_approx("bytes", 0.95).alias("p95_response_size"),
        percentile_approx("bytes", 0.99).alias("p99_response_size"),
        max("bytes").alias("max_response_size"),
        min("bytes").alias("min_response_size")
    )

    # 6. HTTP Method Analysis
    method_metrics = df.groupBy(
        window(col("timestamp"), "5 minutes"),
        "method"
    ).agg(
        count("*").alias("method_count"),
        avg("bytes").alias("method_avg_bytes")
    )

    # Join all metrics
    final_metrics = time_metrics \
        .join(path_metrics, ["window"], "left") \
        .join(browser_metrics, ["window"], "left") \
        .join(geo_metrics, ["window"], "left") \
        .join(perf_metrics, ["window"], "left") \
        .join(method_metrics, ["window"], "left") \
        .select(
            col("timestamp"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("request_count"),
            col("avg_bytes"),
            col("total_bytes"),
            col("error_count"),
            col("success_count"),
            col("unique_visitors"),
            col("path"),
            col("path_hits"),
            col("path_avg_bytes"),
            col("path_errors"),
            col("browser_type"),
            col("browser_requests"),
            col("browser_avg_bytes"),
            col("ip_class"),
            col("ip_requests"),
            col("unique_ips"),
            col("p95_response_size"),
            col("p99_response_size"),
            col("max_response_size"),
            col("min_response_size"),
            col("method"),
            col("method_count"),
            col("method_avg_bytes")
        )

    return final_metrics

def calculate_moving_averages(df: DataFrame) -> DataFrame:
    # Tính moving averages cho các metrics quan trọng
    window_spec = Window.orderBy("window_start").rowsBetween(-2, 0)
    
    return df.withColumn(
        "avg_requests_ma3", avg("request_count").over(window_spec)
    ).withColumn(
        "avg_bytes_ma3", avg("avg_bytes").over(window_spec)
    ).withColumn(
        "error_rate_ma3", avg(col("error_count") / col("request_count")).over(window_spec)
    )

def detect_anomalies(df: DataFrame) -> DataFrame:
    # Phát hiện bất thường dựa trên ngưỡng
    return df.withColumn(
        "is_traffic_spike",
        when(col("request_count") > col("avg_requests_ma3") * 2, True).otherwise(False)
    ).withColumn(
        "is_error_spike",
        when(col("error_count") > col("avg_requests_ma3") * 0.1, True).otherwise(False)
    ).withColumn(
        "is_bandwidth_spike",
        when(col("total_bytes") > col("avg_bytes_ma3") * 3, True).otherwise(False)
    )