from confluent_kafka import Producer
import time

# Cấu hình Kafka
conf = {
    'bootstrap.servers': 'localhost:9094',  # Sử dụng cổng OUTSIDE để gửi dữ liệu
    'client.id': 'file-producer'
}

producer = Producer(conf)

def delivery_report(err, msg):
    """ Báo cáo khi gửi tin nhắn thành công hoặc thất bại """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Đọc file và gửi dữ liệu vào Kafka
with open("../data/access.log", "r") as file:
    for line in file:
        line = line.strip()
        if line:
            producer.produce("logs-topic", key="log", value=line, callback=delivery_report)
            producer.flush()
            time.sleep(1)  # Giả lập gửi dữ liệu theo thời gian thực
