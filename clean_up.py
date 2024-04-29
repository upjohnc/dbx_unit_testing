# Databricks notebook source

schema_name = "chad_invoice_processing"
t = spark.sql(f"show tables in {schema_name};").collect()

for i in t:
    spark.sql(f"drop table {schema_name}.{i.tableName};")

spark.sql(f"drop schema {schema_name};")
