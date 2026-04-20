from kafka import KafkaConsumer
import report_pb2

broker = 'localhost:9092'
topic_name = 'stock_prices'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=broker,
    group_id='debug'
)

for msg in consumer:
    report = report_pb2.Report()
    report.ParseFromString(msg.value)
    print({
        'ticker': report.ticker,
        'date': report.date,
        'price': f'{report.price:.4f}',
        'partition': msg.partition
    })
