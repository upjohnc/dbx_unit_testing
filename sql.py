import os
import sys

from databricks import sql

token = os.environ.get("TOKEN")

if token is None:
    print("Need access token")
    sys.exit()

connection = sql.connect(
    server_hostname="dbc-d02e2229-51a4.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/0b83bc4c230ceef2",
    access_token=token,
)

cursor = connection.cursor()

# cursor.execute("SELECT * from hive_metastore.bundle_project_dev.filtered_taxis limit 10")
# print(cursor.fetchall())
cursor.execute("drop table if exists hive_metastore.bundle_project_dev.filtered_taxis;")
cursor.execute("drop schema if exists hive_metastore.bundle_project_dev;")

cursor.close()
connection.close()
