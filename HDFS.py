from hdfs import InsecureClient
import pandas as pd
import io
from fastavro import writer, parse_schema
import os
# Create an HDFS client
client = InsecureClient('http://localhost:9870/', user='root')

csv_files=['fact_apperance.csv','Dim_Players.csv','Dim_clubs.csv','Dim_competitions.csv','Dim_games.csv']
for file in csv_files:
    df = pd.read_csv(file)
    base_name = os.path.splitext(file)[0]
    with client.write(f'HDFS_Project1/{base_name}.csv') as csv_writer:
        df.to_csv(csv_writer, index=False)

    # --- Parquet Upload ---
    # Create in-memory Parquet file
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow')
    parquet_buffer.seek(0)  # Reset buffer position

    # Write to HDFS
    with client.write(f'HDFS_Project1/{base_name}.parquet', overwrite=True) as parquat_writer:
        parquat_writer.write(parquet_buffer.getvalue())

    # --- Avro Upload ---
    def generate_avro_schema(df):
        schema = {
            "type": "record",
            "name": "GameRecord",
            "fields": []
        }
        for column in df.columns:
            dtype = df[column].dtype
            # Map pandas dtype to Avro type
            if dtype == "int64":
                avro_type = "long"
            elif dtype == "float64":
                avro_type = "double"
            elif dtype == "bool":
                avro_type = "boolean"
            else:
                avro_type = "string"  # Default to string for objects/datetimes
            
            # Handle nullable fields (if column has NaN values)
            if df[column].isnull().any():
                avro_type = ["null", avro_type]
            
            schema["fields"].append({
                "name": column,
                "type": avro_type
            })
        return schema

    # Convert DataFrame to Avro-compatible records
    records = df.replace({pd.NA: None}).to_dict('records')  # Handle NaN/NaT

    # Generate and validate Avro schema
    avro_schema = generate_avro_schema(df)
    parsed_schema = parse_schema(avro_schema)

    # Write Avro to HDFS
    avro_buffer = io.BytesIO()
    writer(avro_buffer, parsed_schema, records)
    avro_buffer.seek(0)

    with client.write(f'HDFS_Project1/{base_name}.avro', overwrite=True) as avro_writer:
        avro_writer.write(avro_buffer.getvalue())

print("Files uploaded to HDFS: CSV, Parquet, and Avro!")

