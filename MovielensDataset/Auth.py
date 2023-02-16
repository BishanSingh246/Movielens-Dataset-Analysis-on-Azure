# Databricks notebook source
spark

# COMMAND ----------

# Connect to data lake storage
# spark.conf.set("fs.azure.account.key.<Storage account name>.dfs.core.windows.net","<Access key>")
spark.conf.set("fs.azure.account.key.dlsamovielens.dfs.core.windows.net","41eUkaE2GeJzJK3HgZQF8SOz8WdysvScRYMDYa+yXdgbvRZHmUYYCuFY0mLhFEBkeBWdCxOYr4Zf+AStcrRWLQ==")

# COMMAND ----------

# Acess files in the container
# dbutils.fs.ls("abfss://<container-name>@<Storage account name>.dfs.core.windows.net/")
dbutils.fs.ls("abfss://dl-container@dlsamovielens.dfs.core.windows.net/")

# COMMAND ----------

# Create mount point on Databrick file system
dbutils.fs.mount(
#     source = "wasbs://<container-name>@<Storage account name>.blob.core.windows.net"
    source = "wasbs://dl-container@dlsamovielens.blob.core.windows.net",
    mount_point = "/mnt/movielens",
#     extra_configs = {"fs.azure.sas.<container name>.<storage name>.blob.core.windows.net":"<sas token from container>"}
    extra_configs = {"fs.azure.sas.dl-container.dlsamovielens.blob.core.windows.net":"sp=r&st=2023-02-15T19:52:58Z&se=2023-02-16T03:52:58Z&spr=https&sv=2021-06-08&sr=c&sig=d8IqcXB3gfcTSJzWV7%2BUHwlZZfb8rRIg8rCde1kX5kY%3D"})

# COMMAND ----------

# if mount point is created alredy the unmount a mount point
# dbutils.fs.unmount("/mnt/<mount point naem>")
# dbutils.fs.unmount("/mnt/movielens")

# COMMAND ----------

# mount point path query
dbutils.fs.ls("/mnt/")