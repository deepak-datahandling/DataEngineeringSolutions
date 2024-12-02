# Live Weather Report Analysis using Microsoft Fabric

This project demonstrates an end-to-end implementation of live weather report analysis for various countries using Microsoft Fabric. The following steps outline the process:

## Table of Contents
1. [Project Overview](#project-overview)
2. [Services and Technologies Used](#services-and-technologies-used)
3. [Step-by-Step Implementation](#step-by-step-implementation)
4. [Real-Time Workflow](#real-time-workflow)
6. [Visuals](#visuals)
7. [Alternative Batch Processing](#additional-batch-processing)
## Project Overview
This project involves collecting weather data from various regions, processing it using Spark, and visualizing the results in Power BI. The data pipeline leverages Azure Data Factory (ADF), Synapse Notebooks, OneLake, and Power BI for seamless data integration and visualization.

## Services and Technologies Used
- **Fabric Services**: 
  - Azure Data Factory (ADF)
  - Synapse Notebooks (for data stream processing)
  - OneLake (for data storage)
  - Semantic Model (for data modeling and relationships)
  - Power BI (for data visualization using DAX queries)
- **Technical Stack**:
  - PySpark
  - Spark-Structured Streaming
  - DAX Queries (beginner level)
  - Data Visualization using Power BI

## Step-by-Step Implementation

### 1. Create Country Table
- **Description**: Create a table containing country region, country code, and country name details.
- **Implementation**: Use REST API to fetch country details, clean the data using PySpark, and save it as a Delta table.

### 2. Build ADF Pipeline
- **Description**: Create an ADF pipeline to fetch weather details using REST API.
- **Steps**:
  1. **Inputs**: Country region and API Key.
  2. **Lookup Activity**: Trigger a stored procedure to get country details.
  3. **ForEach Loop**: Iterate over countries and fetch weather details in parallel.
  4. **Copy Data Activity**: Load JSON responses into OneLake Lakehouse path.

### ADF Pipeline

![ADF Pipeline](https://github.com/user-attachments/assets/ada2d45f-9bba-478c-9d62-330f8f4a2fd2)

### 3. Data Processing with Synapse Notebooks
- **Description**: Process and load weather data into different tables using PySpark and Spark Structured Streaming.

### 4. Create Semantic Model
- **Description**: Build a semantic model with table relationships.
- **Steps**: 
  - Use Power BI to create relationships between tables and define measures.
  - **Table Names and Relationships**:
    - **CountryDetails**: Contains region, country, and country code details.
    - **countrymstr**: Fact table storing primary key columns like country code, atmosphere ID, weather ID, and load timestamp.
    - **countrydetfinal**: Dimension table with detailed country information such as city, latitude, and longitude.
    - **weatherdet**: Dimension table with weather description details.
    - **atmospheredet**: Dimension table with SCD Type 2, storing weather details like humidity, temperature, wind speed, wind direction, and cloud cover.

  - **Relationships**:
    - `countrymstr (country code)` -> one to one -> `countrydetfinal (country code)`
    - `countrymstr (weather id)` -> many to one -> `weatherdet (weather id)`
    - `countrymstr (atmosphere id)` -> one to many -> `atmospheredet (atmosphere id)`

### Data Modeling Relationship

![ADF Pipeline](https://github.com/user-attachments/assets/107c4702-4400-4105-9f52-a89d27d3bce9)

### 5. Power BI Report
- **Description**: Create a Power BI report to visualize weather data.
- **Steps**: Use the semantic model to build and publish a weather report.
- **Power BI**: Implement DAX queries for calculations and visualizations.

### Data Lineage

![Data Lineage](https://github.com/user-attachments/assets/1f1b3985-8fca-40d4-81b0-99efa6560ebd)

## Real-Time Workflow

1. **Schedule ADF Pipeline**: Set the pipeline to run every 15 minutes.
2. **Start Streaming Notebook**: Initiate the Spark notebook for data processing before the ADF pipeline starts.
3. **Data Ingestion and Processing**:
   - ADF loads JSON files into OneLake.
   - Spark Structured Streaming processes the data as it lands in OneLake.
4. **Power BI Report Update**: Refresh the Power BI report to display the latest weather data.


## Visuals

### Published Report
- You can view the published Power BI weather report [here](https://app.fabric.microsoft.com/view?r=eyJrIjoiNWFkZGUwMjMtNWQyNy00NjE4LWFjNTMtOGFhNTM2ODc3ZDZkIiwidCI6IjI5OTZmNDI3LTkyOTctNDY1ZS04YmYwLWYyMTIyYzAzMWQxYyIsImMiOjl9).

### Final Dashboard
- Final View:
 ![Final Report](https://github.com/user-attachments/assets/ed8643c6-9073-4d85-bf9c-649fb0a4f63c)

- Normal View:
  ![Final Report 1](https://github.com/user-attachments/assets/1cf9f17f-e5ad-4426-9f8f-d95d01c7f8d0)

- Drill Down View:
  ![Final Report 2](https://github.com/user-attachments/assets/d6e8a8bb-b2a4-436b-9884-addf641c4e35)

## Alternative Batch Processing

### Batch Processing Notebook
- **Description**: An additional notebook for batch processing is available as an alternative to the streaming notebook.
- **Steps**:
  - Configure the batch processing notebook to process data in batches after it is loaded into OneLake.
  - Add this notebook as a final step in the ADF pipeline for batch processing if streaming is not preferred.

---

**This process ensures real-time weather data analysis and visualization across various regions.**
