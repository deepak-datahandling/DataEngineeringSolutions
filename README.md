# Data Validation Across Databricks Environments

This repository contains the code for performing data validation between identical Databricks tables located in different environments (e.g., Dev and Prod). The project leverages PySpark and Spark SQL to ensure data consistency across environments.

## Project Overview

The project involves validating data between two sets of tables, each containing 100 tables, split into two batches for parallel processing. The validation process includes generating a token for the Prod environment, running queries to fetch Prod data in the Dev environment, and performing a minus query to compare the data at the table level.

## Technologies Used

- PySpark
- Spark SQL
- Databricks
  
## Process Flow

### Steps:

1. **Generate Prod Token**: Create and store the Prod token in a secure location (secret scope or elsewhere).
2. **Fetch Prod Data**: Use the Prod token and cluster details to retrieve data from the Prod into the Dev environment.
3. **Data Validation**: Execute a minus query to compare the tables at the data level and store the results in a validation results table.
4. **Audit**: Store the validation time for each table in an audit table for optimization.

### Trigger Notebook

- A trigger notebook is created to check the status of the Prod cluster and activate it if it's not live. This reduces the waiting time for validation scripts.

### Workflow Tasks

The workflow consists of three tasks:
1. **Trigger Notebook**: Ensures the Prod cluster is live before validation.
2. **Parallel Validation Notebooks**: Two parallel notebooks perform the validation on two sets of 50 tables each.

## Repository Structure

- **notebooks/**: Contains the notebooks for data validation and the trigger notebook.
  - `Delta_Table_Validation.ipynb`: Notebook for validating the tables 
  - `Cluster_Trigger.ipynb`: Notebook to check and trigger the Prod cluster.
