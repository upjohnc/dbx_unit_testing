# Databricks notebook source
t = spark.sql("show tables in chad_upjohn_dbx_unit_tests;").collect()

for i in t:
    spark.sql(f'drop table chad_upjohn_dbx_unit_tests.{i.tableName};')

spark.sql(f'drop schema chad_upjohn_dbx_unit_tests;')

