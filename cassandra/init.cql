-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS logs_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE logs_analytics;

-- Raw logs table
CREATE TABLE IF NOT EXISTS raw_logs (
    ip text,
    timestamp timestamp,
    method text,
    url text,
    protocol text,
    status int,
    bytes bigint,
    referrer text,
    user_agent text,
    day_bucket date,
    PRIMARY KEY ((day_bucket), timestamp, ip)
) WITH CLUSTERING ORDER BY (timestamp DESC);