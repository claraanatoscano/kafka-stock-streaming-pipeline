import os
import time
import sqlalchemy
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError
import report_pb2

broker = 'localhost:9092'
topic_name = 'stock_prices'
project = os.environ.get("PROJECT", "p7")
db_url = f"mysql+pymysql://root:abc@{project}-mysql-1/CS544"

def init_topic():
    admin = KafkaAdminClient(bootstrap_servers=broker)
    try:
        admin.delete_topics([topic_name])
        print("Deleted existing topic, sleeping 3s...")
        time.sleep(3)
    except UnknownTopicOrPartitionError:
        pass
    admin.create_topics([NewTopic(name=topic_name, num_partitions=4, replication_factor=1)])
    print("Topic created.")
    admin.close()

def main():
    init_topic()

    engine = sqlalchemy.create_engine(db_url)
    producer = KafkaProducer(
        bootstrap_servers=broker,
        retries=10,
        acks='all'
    )

    last_id = 0
    while True:
        with engine.connect() as conn:
            rows = conn.execute(
                sqlalchemy.text("SELECT id, ticker, date, price FROM stock_prices WHERE id > :last_id ORDER BY id"),
                {"last_id": last_id}
            ).fetchall()

        for row in rows:
            id_, ticker, date, price = row
            report = report_pb2.Report(
                date=str(date),
                price=float(price),
                ticker=ticker
            )
            producer.send(
                topic_name,
                key=ticker.encode('utf-8'),
                value=report.SerializeToString()
            )
            last_id = id_

        if not rows:
            time.sleep(0.5)

if __name__ == '__main__':
    main()
