from hdfs import InsecureClient
import pandas as pd
import io
import time
from fastavro import writer, parse_schema

client = InsecureClient('http://localhost:9870', user='root')
df = pd.read_csv("Dim_games.csv")

# CSV Write
start = time.time()
with client.write("/test_data1/data.csv", overwrite=True) as csv_writer:
    df.to_csv(csv_writer, index=False)
csv_write_time = time.time() - start

# Parquet Write (Fixed)
start = time.time()
parquet_buffer = io.BytesIO()
df.to_parquet(parquet_buffer, engine='pyarrow')
parquet_buffer.seek(0)
with client.write("/test_data1/data.parquet", overwrite=True) as hdfs_writer:
    hdfs_writer.write(parquet_buffer.getvalue())
parquet_write_time = time.time() - start

# Avro Write
schema = {
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": col, "type": "string"} for col in df.columns
    ]
}
parsed_schema = parse_schema(schema)
records = df.astype(str).to_dict('records')

start = time.time()
avro_buffer = io.BytesIO()
writer(avro_buffer, parsed_schema, records)
avro_buffer.seek(0)
with client.write("/test_data1/data.avro", overwrite=True) as hdfs_writer:
    hdfs_writer.write(avro_buffer.getvalue())
avro_write_time = time.time() - start


# -------------------------------------------
# CSV Read Test
# -------------------------------------------
start = time.time()
with client.read("/test_data1/data.csv") as reader:
    csv_df = pd.read_csv(reader)
csv_read_time = time.time() - start

# -------------------------------------------
# Parquet Read Test
# -------------------------------------------
start = time.time()
with client.read("/test_data1/data.parquet") as reader:
    parquet_buffer = io.BytesIO(reader.read())  # Read into memory
    parquet_df = pd.read_parquet(parquet_buffer, engine='pyarrow')
parquet_read_time = time.time() - start


# -------------------------------------------
# Avro Read Test
# -------------------------------------------
from fastavro import reader as avro_reader

start = time.time()
with client.read("/test_data1/data.avro") as hdfs_reader:
    avro_buffer = io.BytesIO(hdfs_reader.read())
    avro_records = list(avro_reader(avro_buffer))
    avro_df = pd.DataFrame(avro_records)
avro_read_time = time.time() - start

print(f"""
=== Write Times ===
CSV:     {csv_write_time:.2f}s
Parquet: {parquet_write_time:.2f}s 
Avro:    {avro_write_time:.2f}s
""")

print(f"""
=== Read Times ===
CSV:     {csv_read_time:.2f}s
Parquet: {parquet_read_time:.2f}s 
Avro:    {avro_read_time:.2f}s
""")