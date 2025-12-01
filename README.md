<h1 align="center">Real-time Banking Data Warehouse</h1>

<p align="center">
  <a href="README.md">English</a> ·
  <a href="README.vi.md">Tiếng Việt</a>
</p>

This project implements an end-to-end **Real-time Data Engineering** pipeline for a Banking System. It simulates high-volume transactional data, captures changes in real-time using **CDC (Change Data Capture)**, and builds a scalable Data Warehouse following the **ELT** paradigm.

The pipeline is fully containerized using Docker, utilizing **PostgreSQL** for storage, **Debezium** & **Kafka** for streaming, **Airflow** for orchestration, and **dbt** for transformation on **Snowflake**.

# Architecture Overview
![Architecture Diagram](./architecture/architecture_diagram.svg)
The pipeline is designed to handle the complete lifecycle of data:
1. **Source Layer (OLTP):** A Python script generates synthetic banking data (Customers, Accounts, Transactions) into a **PostgreSQL** database.
2. **Ingestion Layer (CDC Streaming):**
    * **Debezium** captures row-level changes (INSERT, UPDATE, DELETE) from Postgres WAL logs.
    * Data is streamed into **Apache Kafka** topics.
    * A Custom Python Sink Connector consumes Kafka messages, batches them into **Parquet** files, and uploads them to **MinIO** (S3-compatible Data Lake).
3. **Orchestration & Loading (ELT):**
    * **Apache Airflow** orchestrates the data movement.
    * It downloads Parquet files from MinIO (Landing Zone).
    * Loads data into **Snowflake**'s `RAW` schema using `COPY INTO` with `VARIANT` data type support.
    * Moves processed files to an Archive Zone for incremental loading efficiency.
4. **Transformation Layer (dbt):**
    * **Staging:** Deduplicates CDC logs to retrieve the latest state of data.
    * **Snapshots:** Implements **SCD Type 2 (Slowly Changing Dimensions)** for the `accounts` table to track historical balance changes.
    * **Marts:** Models data into a **Star Schema** with a Bridge Table to handle complex Many-to-Many relationships.