import os, sys, json, tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from kafka import KafkaConsumer, TopicPartition
from subprocess import check_output, run
import report_pb2

os.environ["CLASSPATH"] = str(check_output(
    [os.environ["HADOOP_HOME"] + "/bin/hdfs", "classpath", "--glob"]), "utf-8")

broker = 'localhost:9092'
topic_name = 'stock_prices'
hdfs_base = 'hdfs://boss:9000'
hdfs_data_dir = '/data'

def ensure_hdfs_dir():
    run(["hdfs", "dfs", "-mkdir", "-p", f"{hdfs_base}{hdfs_data_dir}"])

def write_to_hdfs(local_path, hdfs_path):
    # Remove existing file if it exists, then copy local file to HDFS
    run(["hdfs", "dfs", "-rm", "-f", hdfs_path])
    run(["hdfs", "dfs", "-put", local_path, hdfs_path])

def checkpoint_path(partition_id):
    return f'/src/partition-{partition_id}.json'

def load_checkpoint(partition_id):
    path = checkpoint_path(partition_id)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None

def save_checkpoint(partition_id, batch_id, offset):
    with open(checkpoint_path(partition_id), 'w') as f:
        json.dump({"batch_id": batch_id, "offset": offset}, f)

def main():
    if len(sys.argv) != 2:
        print("Usage: python consumer.py <partition_number>")
        sys.exit(1)
    partition_id = int(sys.argv[1])

    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        group_id=None,
        enable_auto_commit=False,
        max_poll_records=5000
    )

    tp = TopicPartition(topic_name, partition_id)
    consumer.assign([tp])

    checkpoint = load_checkpoint(partition_id)
    if checkpoint:
        batch_id = checkpoint['batch_id'] + 1
        consumer.seek(tp, checkpoint['offset'])
        print(f"Resuming from offset {checkpoint['offset']}, batch {batch_id}")
    else:
        batch_id = 0
        consumer.seek(tp, 0)
        print("Starting from offset 0")

    ensure_hdfs_dir()

    while True:
        records = consumer.poll(timeout_ms=2000)
        msgs = records.get(tp, [])
        if not msgs:
            continue

        rows = []
        for msg in msgs:
            report = report_pb2.Report()
            report.ParseFromString(msg.value)
            rows.append({'date': report.date, 'price': report.price, 'ticker': report.ticker})

        df = pd.DataFrame(rows, columns=['date', 'price', 'ticker'])
        table = pa.Table.from_pandas(df, preserve_index=False)

        # Write to local temp file first, then copy to HDFS
        hdfs_path = f'{hdfs_base}{hdfs_data_dir}/partition-{partition_id}-batch-{batch_id}.parquet'
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            pq.write_table(table, tmp_path)
            write_to_hdfs(tmp_path, hdfs_path)
            print(f"Wrote {len(rows)} rows to {hdfs_path}")
        finally:
            os.unlink(tmp_path)

        offset = consumer.position(tp)
        save_checkpoint(partition_id, batch_id, offset)
        batch_id += 1

if __name__ == '__main__':
    main()
