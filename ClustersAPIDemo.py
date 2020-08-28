# Databricks notebook source
from databricks_cli.sdk.api_client import ApiClient
apiclient = ApiClient(token = dbutils.entry_point.getDbutils().notebook().getContext().apiToken().get(),
                   host = dbutils.entry_point.getDbutils().notebook().getContext().apiUrl().get())

# COMMAND ----------

apiclient.perform_query("GET", "/clusters/list?state=RUNNING")

# COMMAND ----------

from databricks_cli.clusters.api import ClusterApi
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

clusters_api = ClusterApi(apiclient)

# COMMAND ----------

data = clusters_api.list_clusters()
rdd = sc.parallelize(data['clusters']).map(lambda x: json.dumps(x))
# 

# COMMAND ----------

raw_clusters_df = spark.read.json(rdd)

# COMMAND ----------

parsed_clusters = raw_clusters_df.filter(col('init_scripts').isNotNull())
display(parsed_clusters)

# COMMAND ----------

display(raw_clusters_df)

# COMMAND ----------

clusters_api.list_clusters()

# COMMAND ----------

