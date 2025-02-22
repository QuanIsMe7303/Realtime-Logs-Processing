from confluent_kafka import Producer
import time
import json
import random
import os

POSITION_FILE = "/app/position/last_position.json"

def save_position(position):
    with open(POSITION_FILE, 'w') as f:
        json.dump({'position': position}, f)

def load_position():
    try:
        with open(POSITION_FILE, 'r') as f:
            data = json.load(f)
            return data.get('position', 0)
    except (FileNotFoundError, json.JSONDecodeError):
        return 0

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    conf = {
        'bootstrap.servers': 'kafka:29092',
        'client.id': 'file-producer'
    }
    producer = Producer(conf)

    last_position = load_position()
    print(f"Starting from position: {last_position}")

    with open("/app/data/data.log", "r") as file:
        file.seek(last_position)
        
        while True:
            current_position = file.tell()
            line = file.readline()
            
            if not line:
                time.sleep(1)
                file.seek(current_position)
                continue
                
            line = line.strip()
            if line:
                producer.produce("logs-topic", 
                               key="log", 
                               value=line, 
                               callback=delivery_report)
                producer.flush()
                save_position(file.tell())
                time.sleep(random.uniform(0, 1))

if __name__ == "__main__":
    main()