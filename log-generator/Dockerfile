FROM python:3.9

COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app

CMD ["python", "producer.py"]
