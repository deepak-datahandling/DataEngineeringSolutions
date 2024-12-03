# Open Source - Medical Data Streaming and Processing with Databricks

This repository contains the code for a hackathon project focused on reading and processing medical details from XML documents continuously arriving in a storage location such as ADLS or Databricks DBFS. The project utilizes Spark Structured Streaming, PySpark, Spark SQL, and Delta tables to build comprehensive dashboards on Databricks.

## Project Overview

The primary goal of this project is to perform real-time data streaming and processing of medical XML files, leveraging PySpark and Spark SQL. The processed data is stored in Delta tables, which are then used to create insightful Databricks dashboards.

## Technologies Used

- PySpark
- Spark SQL
- Spark Structured Streaming
- Scala
- Delta Tables
- Databricks

## Process Flow

### Steps:

1. **Schema Generation**:
   - **Tool**: Scala
   - **Description**: Generate the XML schema using Scala code to define the structure needed for streaming.

2. **Data Streaming and Processing**:
   - **Tool**: PySpark and Spark SQL
   - **Description**: Stream XML files from the storage location, process them in downstream DataFrames, and store the results in Delta tables.

3. **Data Storage**:
   - **Tool**: Delta Tables
   - **Description**: Store the processed data in Delta tables for further analysis and dashboard creation.

4. **Dashboard Creation**:
   - **Tool**: Databricks
   - **Description**: Build dashboards to visualize the number of procedures (operations) performed in a particular hospital over the last 3 months and highlight the top 5 procedures across all hospitals.

## Repository Structure

- **notebooks/**: Contains the streaming notebook.
  - `xml_data_streaming.ipynb`: Notebook for streaming and processing the XML medical details documents.
- **ddl_scripts/**: Contains SQL scripts for creating Delta tables.
  - `create_table_procedures.sql`: SQL script to create the Delta table for storing medical procedures.

