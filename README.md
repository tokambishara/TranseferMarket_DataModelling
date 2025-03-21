# **Transfer Market_Data Modeling**  

## **Overview**  
This project structures the TransferMarket dataset using a **star schema** and evaluates different **data storage formats** (CSV, Parquet, Avro) in terms of **compression, read/write performance, and storage efficiency** on **HDFS**.  

## **Data Modeling**  
The dataset is designed around two key business processes:  
- **Player Appearances:** Tracks players in games with dimensions like player, game, competition, and club.
  ![image](https://github.com/user-attachments/assets/2ea10bea-532d-4068-86eb-61cd0b92f15d)

- **Player Transfers:** Records player transfers between clubs, linking players and teams.
  ![image](https://github.com/user-attachments/assets/0d8b78c8-d634-467c-b977-ea759ed79ff1)


Python-based transformations were applied to align the data with the star schema, and results were stored as **CSV files** before format conversion.  

## **Format Conversion & Storage**  
- **Parquet:** Converted using `pyarrow` library.
- **Avro:** Converted using `fastavro` library. 
- **HDFS Deployment:** All formats were uploaded to an **HDFS cluster** using **Docker & Python’s hdfs library** for benchmarking.  

## **Performance Comparison**  
- **File Size:** **Parquet (Brotli)** was the most space-efficient, while **CSV** was the largest.  
- **Write Speed:** **Parquet (Snappy)** had the fastest write time, CSV was the slowest.  
- **Read Speed:** **Parquet (Brotli)** outperformed all formats, with **CSV** being the slowest.  

This project demonstrates efficient **data modeling, transformation, and storage optimization** for large datasets using modern **big data technologies**.  
