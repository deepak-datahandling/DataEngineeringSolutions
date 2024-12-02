-- Databricks notebook source
USE DATABASE med;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS med.Proc_Details(
  PID long,
  Name string,
  Proc_Date date,
  Status string
) USING DELTA LOCATION "/mnt/medblob/med-tables/Proc_Details/"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS med.Proc_Addr_Details(
  PID long NOT NULL,
  Hospital string,
  street string,
  city string,
  state string,
  country string,
  postalCode long
) USING DELTA LOCATION "/mnt/medblob/med-tables/Proc_Addr_Details/"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS med.Proc_Addr_Details_managed(
  PID long NOT NULL,
  Hospital string,
  Street string,
  City string,
  State string,
  Country string,
  PostalCode long
);
CREATE TABLE IF NOT EXISTS med.Proc_Details_managed(
  PID long,
  Name string,
  Proc_Date date,
  Status string
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS med.Proc_Details_managed(
  PID long,
  Name string,
  Proc_Date date,
  Status string
)