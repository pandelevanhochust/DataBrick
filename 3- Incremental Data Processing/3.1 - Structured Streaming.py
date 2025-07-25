# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading Stream

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog workspace

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema default

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers (
# MAGIC   customer_id STRING PRIMARY KEY,
# MAGIC   name STRING,
# MAGIC   email STRING
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TABLE books (
# MAGIC   book_id STRING PRIMARY KEY,
# MAGIC   title STRING,
# MAGIC   genre STRING,
# MAGIC   price DOUBLE
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TABLE orders (
# MAGIC   order_id STRING PRIMARY KEY,
# MAGIC   customer_id STRING,
# MAGIC   order_timestamp TIMESTAMP,
# MAGIC   books ARRAY<STRUCT<book_id: STRING, quantity: INT>>,
# MAGIC   FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO customers VALUES
# MAGIC   ('c1', 'Alice', 'alice@example.com'),
# MAGIC   ('c2', 'Bob', 'bob@example.com');
# MAGIC
# MAGIC INSERT INTO books VALUES
# MAGIC   ('b1', 'The Hobbit', 'Fantasy', 10.0),
# MAGIC   ('b2', '1984', 'Fiction', 8.5),
# MAGIC   ('b3', 'Sapiens', 'Non-fiction', 12.0);
# MAGIC
# MAGIC INSERT INTO orders VALUES
# MAGIC   ('o1', 'c1', current_timestamp(), ARRAY(
# MAGIC     STRUCT('b1' AS book_id, 1 AS quantity),
# MAGIC     STRUCT('b2' AS book_id, 2 AS quantity)
# MAGIC   )),
# MAGIC   ('o2', 'c2', current_timestamp(), ARRAY(
# MAGIC     STRUCT('b3' AS book_id, 1 AS quantity)
# MAGIC   ));
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS checkpoint_volume
# MAGIC

# COMMAND ----------

(spark.readStream
        .table("books")
        .createOrReplaceTempView("workspace.default.books_streaming_output")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  books_streaming_tmp

# COMMAND ----------

(
  spark.readStream
    .table("orders")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "dbfs:/Volumes/workspace/default/checkpoint_volume/orders_streaming")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("workspace.default.orders_streaming_output")
)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Displaying Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_streaming_output

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(order_id), customer_id AS total_books
# MAGIC FROM orders_streaming_output
# MAGIC GROUP BY customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Unsupported Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * FROM orders_streaming_output
# MAGIC  ORDER BY size(books)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Persisting Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )

# COMMAND ----------

(spark.readStream.table("books")                               
      .writeStream  
      .trigger(processingTime='availableNow = True')
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/Volumes/workspace/default/checkpoint_volume/books_tmp_streaming")
      .toTable("workspace.default.books_streaming_tmp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding New Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming in Batch Mode 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts
