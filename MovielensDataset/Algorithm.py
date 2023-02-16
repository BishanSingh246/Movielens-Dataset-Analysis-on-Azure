# Databricks notebook source
# MAGIC %run /MovielensDataset/Auth

# COMMAND ----------

# import required libraries
import datetime
import pyspark.sql.functions as f
import pyspark.sql.types
import pandas as pd

from  pyspark.sql.functions import year, month, dayofmonth
from  pyspark.sql.functions import unix_timestamp, from_unixtime
from  pyspark.sql import Window
from  pyspark.sql.functions import rank, min 

# COMMAND ----------

# Acess files in the container
# dbutils.fs.ls("abfss://<container-name>@<Storage account name>.dfs.core.windows.net/")
dbutils.fs.ls("abfss://dl-container@dlsamovielens.dfs.core.windows.net/")

# COMMAND ----------

# list all tha mounts
dbutils.fs.mounts()

# COMMAND ----------

# unzip the file using the Databrick file system(DBFS) and copy the file from dbfs into local file system
dbutils.fs.cp("dbfs:/mnt/movielens/ml-latest-small.zip", "file:/tmp/ml-latest-small.zip")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /tmp/ml-latest-small.zip

# COMMAND ----------

# for removing 

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r /tmp/ml-latest-small.zip

# COMMAND ----------

# unzip the file using the Databrick file system(DBFS) and copy the file from dbfs into local file system
dbutils.fs.cp("dbfs:/mnt/movielens/ml-latest-small.zip", "file:/tmp/ml-latest-small.zip")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /tmp/ml-latest-small.zip

# COMMAND ----------

# unziping the files into tmp

# COMMAND ----------

# MAGIC %sh 
# MAGIC unzip /tmp/ml-latest-small.zip -d /tmp

# COMMAND ----------

# cheking the files after unziping

# COMMAND ----------

dbutils.fs.ls("file:/tmp/ml-latest-small/")

# COMMAND ----------

# copying files from local file system to azure datalake container
dbutils.fs.cp("file:/tmp/ml-latest-small/movies.csv", "abfss://dl-container@dlsamovielens.dfs.core.windows.net/movies.csv")
dbutils.fs.cp("file:/tmp/ml-latest-small/ratings.csv", "abfss://dl-container@dlsamovielens.dfs.core.windows.net/ratings.csv")
dbutils.fs.cp("file:/tmp/ml-latest-small/tags.csv", "abfss://dl-container@dlsamovielens.dfs.core.windows.net/tags.csv")
dbutils.fs.cp("file:/tmp/ml-latest-small/links.csv", "abfss://dl-container@dlsamovielens.dfs.core.windows.net/links.csv")

# COMMAND ----------

#  %fs is another comand similar to dbutils

# COMMAND ----------

# MAGIC %fs ls abfss://dl-container@dlsamovielens.dfs.core.windows.net/

# COMMAND ----------

# MAGIC %fs ls /mnt/movielens/movies.csv

# COMMAND ----------

# List of files inside source folder in datalake storage

# COMMAND ----------

# MAGIC %fs ls abfss://dl-container@dlsamovielens.dfs.core.windows.net/Source/ml-latest-small.zip/ml-latest-small/

# COMMAND ----------

# if you uploded data in data explorer and created table  

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/

# COMMAND ----------

# creating table or DataFrame from data uploded in DBFS

# COMMAND ----------

"# This is temporary table
# File location and type

#To create table from files uploded in DBFS 
file_location = "/FileStore/tables/movies.csv"

#To create table from files uploded in DBFS 
# file_location = "/mnt/movielens/links.csv"

# #To create table from files uploded in datalake 
# file_location = "/mnt/movielens/Source/ml-latest-small.zip/ml-latest-small/links.csv"


file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_movies = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df_movies)
df_movies.show()
df_movies.count()"

# COMMAND ----------

# Create a view or table

temp_table_name = "movies_csv"
print(temp_table_name)

df_movies.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `links_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "movies"

# Creating permanent table
# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

# create table from files uploded in DBFS 
links = "/mnt/movielens/links.csv"

df_links = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(links)

display(df_links)


# COMMAND ----------

# create table from files uploded in DBFS 
tags = "/mnt/movielens/tags.csv"

df_tags = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(tags)

display(df_tags)

# COMMAND ----------

# create table from files uploded in DBFS 
ratings = "/mnt/movielens/ratings.csv"

df_ratings = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(ratings)

display(df_ratings)
 

# COMMAND ----------

df_movies.count()

# COMMAND ----------

display(df_movies)

# COMMAND ----------

# join movies and rating table using left join
df_movies_with_ratings = df_movies.join(df_ratings,"movieId","left")

display(df_movies_with_ratings)

# COMMAND ----------

#Check if there are no duplicates group by
df_movies_no_dups = df_movies_with_ratings.groupby('movieId').count()
display(df_movies_no_dups)

# COMMAND ----------

# Display single movies by Id
df_movies.where(df_movies["movieId"]==295).display()

# COMMAND ----------

# Display multiple movies by Id
df_movies.where(df_movies.movieId.isin([296,593, 858])).display()

# COMMAND ----------

#inner join with usersId (tags dataframe)
df_movies_with_ratings=df_movies_with_ratings.join(df_tags,['movieID','userId'],'inner')
display(df_movies_with_ratings)

# COMMAND ----------

#Join with ratings with tag
df_ratings_tags=df_ratings.join(df_tags,['movieID'],'inner')
display(df_movies_with_ratings)

# COMMAND ----------

# converting timestamp in proper date and time in ratings
df_ratings_add_tsdate=df_ratings.withColumn("tsdate",f.from_unixtime("timestamp"))

# COMMAND ----------

df_ratings_add_tsdate.display()

# COMMAND ----------

# removing time from tsdate and remaninf it to rating_date
df_selected_rating_date=df_ratings_add_tsdate.select('userid','movieid','rating',f.to_date(unix_timestamp('tsdate','yyyy-MM-dd HH:mm:ss').cast('timestamp')).alias('rating_date'))
df_selected_rating_date.display()

# COMMAND ----------

# group by rating date
df_rating_year=df_selected_rating_date.groupBy('rating_date').count()
df_rating_year.display()

# COMMAND ----------

# average rating 
df_avg_ratings=df_selected_rating_date.groupBy('movieid').mean('rating')
df_avg_ratings.display()

# COMMAND ----------

# avg rating with movies name
df=df_avg_ratings.join(df_movies,'movieid', 'inner')
df=df.withColumnRenamed('avg(rating)', 'avg_rating')
df.display()

# COMMAND ----------

# total rating for a movie
df_total_rating=df_selected_rating_date.groupBy('movieid').count()
df_total_rating.display()

# COMMAND ----------

# movie id count more than 50
df_total_rating=df_total_rating.filter(df_total_rating['count']>50)
df_ratings_filtered=df_selected_rating_date.join(df_total_rating, 'movieid', 'inner')
df_ratings_filtered.display()

# COMMAND ----------

# total count which is having rating more than 50
df_total_rating.count()

# COMMAND ----------

# total rating joined with selected date
df_rating_per_user=df_ratings_filtered.select('userid','movieid','rating').groupBy('userid', 'movieid').max('rating')
df_rating_per_user_movie=df_rating_per_user.join(df_movies,'movieid','inner')
df_rating_per_user_movie=df_rating_per_user_movie.withColumnRenamed('max(rating)', 'max_rating')
df_rating_per_user_movie.display()

# COMMAND ----------

# movies with maximum ratings group by userId and movieId
df_rating_max_rating=df_rating_per_user_movie.groupBy('userid','movieid','title','genres').max('max_rating')
# renaming the the column with max_rating
df_rating_max_rating=df_rating_max_rating.withColumnRenamed('max(max_rating)', 'max_rating')
df_rating_max_rating.display()

# COMMAND ----------

# filter by max rating is grater than 4
df_rating_max_rating=df_rating_max_rating.filter(df_rating_max_rating['max_rating']>=4)
df_rating_max_rating.display()

# COMMAND ----------

#identify best movies per genre
df_movies_per_genere=df_rating_max_rating.groupBy('genres','title').count()
df_movies_per_genere.display()A

# COMMAND ----------

#identify genres of user
df_ratings_genre=df_rating_max_rating.select('userid','title','genres').groupBy('userid','genres').count()
df_ratings_genre.display()

# COMMAND ----------

#latest Trending Movies Over all
df_recent_movie=df_ratings_filtered.groupBy('userid','movieid').agg(f.max(df_ratings_filtered['rating_date']))
df_recent_movie.display()

# COMMAND ----------

df_movies_per_genere=df.groupBy('genres').avg('avg_rating')
display(df_movies_per_genere)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.permanent_table_name;