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
db_url = f"mysql+mysqlconnector://root:abc@{project}-mysql-1/CS544"

def init_topic():
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker)
            break
        except Exception as e:
            print(f"Waiting for Kafka: {e}")
            time.sleep(2)

    try:
        admin.delete_topics([topic_name])
        print("Deleted existing topic, sleeping 3s...")
        time.sleep(3)
    except UnknownTopicOrPartitionError:
        pass
    admin.create_topics([NewTopic(name=topic_name, num_partitions=4, replication_factor=1)])
    print("Topic created.")
    admin.close()

def get_engine():
    while True:
        try:
            engine = sqlalchemy.create_engine(db_url)
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1"))
            print("DB connected.")
            return engine
        except Exception as e:
            print(f"Waiting for DB: {e}")
            time.sleep(2)

def wait_for_data(engine):
    while True:
        try:
            with engine.connect() as conn:
                result = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM stock_prices"))
                count = result.fetchone()[0]
                if count > 0:
                    print(f"DB has {count} rows, starting producer...")
                    return
                else:
                    print("Waiting for data in stock_prices...")
                    time.sleep(1)
        except Exception as e:
            print(f"Error checking data: {e}")
            time.sleep(2)

def main():
    init_topic()
    engine = get_engine()
    wait_for_data(engine)

    producer = KafkaProducer(
        bootstrap_servers=broker,
        retries=10,
        acks='all'
    )

    last_id = 0
    while True:
        try:
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
        except Exception as e:
            print(f"DB error: {e}")
            time.sleep(2)
            engine = get_engine()

if __name__ == '__main__':
    main()
