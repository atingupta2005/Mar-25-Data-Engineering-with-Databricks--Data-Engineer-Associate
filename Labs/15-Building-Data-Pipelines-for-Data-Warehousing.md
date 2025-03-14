# Building Data Pipelines for Data Warehousing  
### **Notebook 1: Batch Data Ingestion & Save as Delta/Parquet**

**Purpose:**  
- Load raw CSV files (customers, orders, products) from the source.
- Write each DataFrame as Delta (or Parquet) files to the data lake.

**Code Example:**

```python
# Notebook 1: Batch Data Ingestion

# Define file paths for raw CSV data (update folder names as needed)
customers_csv_path = "/mnt/data_atin/batch/customers.csv"
orders_csv_path    = "/mnt/data_atin/batch/orders.csv"
products_csv_path  = "/mnt/data_atin/batch/products.csv"

# Ingest CSV files using Spark
df_customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(customers_csv_path)

df_orders = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(orders_csv_path)

df_products = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(products_csv_path)

# Write the ingested data to Delta Lake (or Parquet) for persistence
df_customers.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/delta/batch/customers")

df_orders.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/delta/batch/orders")

df_products.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/delta/batch/products")

# Optionally display a sample record for verification
display(df_customers)
display(df_orders)
display(df_products)
```

---

### **Notebook 2: Batch Data Transformation & Enrichment**

**Purpose:**  
- Read the persisted Delta tables from Notebook 1.
- Clean and join the datasets (e.g., join orders with customers and products).
- Write the enriched data back to the data lake.

**Code Example:**

```python
# Notebook 2: Batch Data Transformation & Enrichment

from pyspark.sql.functions import col

# Read the persisted data from Notebook 1
df_customers = spark.read.format("delta").load("/mnt/delta/batch/customers")
df_orders    = spark.read.format("delta").load("/mnt/delta/batch/orders")
df_products  = spark.read.format("delta").load("/mnt/delta/batch/products")

# Data Cleaning: Cast 'amount' to float and filter out nulls in orders DataFrame
df_orders_clean = df_orders.withColumn("amount", col("amount").cast("float")) \
    .filter(col("amount").isNotNull())

# Join orders with customers on 'customer_id'
orders_customers = df_orders_clean.join(df_customers, on="customer_id", how="inner")

# Join the above with products on 'product_id'
enriched_data = orders_customers.join(df_products, on="product_id", how="inner")

# Write the enriched data to Delta Lake for downstream processing
enriched_data.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/delta/enriched/retail_data")

# Optionally create a temporary view for further SQL operations
enriched_data.createOrReplaceTempView("retail_data")
display(enriched_data)
```

---

### **Notebook 3: Batch Data Aggregation & Final Load into Data Warehouse**

**Purpose:**  
- Read the enriched data from Notebook 2.
- Perform aggregations using Spark SQL (e.g., compute total sales and order counts per customer).
- Write the final aggregated results to a Delta Lake table that serves as the data warehouse.

**Code Example:**

```python
# Notebook 3: Batch Data Aggregation & Delta Lake Write

# Read the enriched data from Notebook 2
enriched_data = spark.read.format("delta").load("/mnt/delta/enriched/retail_data")

# Alternatively, if a temporary view was created in Notebook 2, you can use that:
# spark.sql("SELECT * FROM retail_data")

# Run aggregation using Spark SQL
aggregated_result = spark.sql("""
    SELECT customer_id, 
           SUM(amount) AS total_sales, 
           COUNT(order_id) AS order_count
    FROM retail_data
    GROUP BY customer_id
    ORDER BY total_sales DESC
""")

# Display the aggregated result for verification
display(aggregated_result)

# Write the aggregated result to Delta Lake as the final data warehouse table
delta_table_path = "/mnt/delta/warehouse_sales"

aggregated_result.write.format("delta") \
    .mode("overwrite") \
    .save(delta_table_path)

# Optionally, register the Delta table for interactive querying
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS warehouse_sales
    USING DELTA
    LOCATION '{delta_table_path}'
""")
```

---

## Notebook 4: Streaming Data Ingestion & Transformation

**Purpose:**  
- Ingest streaming JSON data from the source folder.
- Parse the JSON records with a defined schema.
- Aggregate the data (e.g. group by customer and sum order amounts).
- Write the streaming aggregated results continuously to a staging Delta table.

**Code:**

```python
# Notebook 4: Streaming Data Ingestion & Transformation

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Define the streaming source path (update if needed)
streaming_source_path = "/mnt/data_atin/streaming/orders/"

# Read the streaming data (JSON format)
streaming_df = spark.readStream.format("json") \
    .option("maxFilesPerTrigger", 1) \
    .load(streaming_source_path)

# Define the schema for incoming JSON records
stream_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# Parse the JSON data using the defined schema
parsed_stream = streaming_df.select(from_json(col("value"), stream_schema).alias("data")).select("data.*")

# Aggregate the streaming data: group by customer_id and sum the order amounts
streaming_agg = parsed_stream.groupBy("customer_id").sum("amount") \
    .withColumnRenamed("sum(amount)", "total_sales")

# Write the streaming aggregation to a staging Delta table
staging_delta_path = "/mnt/delta/streaming/staging_orders"
checkpoint_staging = "/mnt/delta/checkpoints/staging_orders"

streaming_query = streaming_agg.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", checkpoint_staging) \
    .start(staging_delta_path)

# Optionally display the streaming aggregation for preview
display(streaming_agg)

# For testing, you may run for a fixed duration (e.g., 60 seconds) and then stop:
# streaming_query.awaitTermination(60)
# streaming_query.stop()
```

---

## Notebook 5: Streaming Data Finalization & Write to Data Warehouse

**Purpose:**  
- Read the streaming output staged by Notebook 4 from the Delta table.
- Optionally perform further processing or aggregation.
- Write the final aggregated streaming results to a Delta table that serves as your data warehouse.

**Code:**

```python
# Notebook 5: Streaming Data Finalization & Write to Final Delta Table

# Read the staged streaming data that was written in Notebook 4
staged_streaming_df = spark.read.format("delta").load("/mnt/delta/streaming/staging_orders")

# Display the staged data for verification
display(staged_streaming_df)

# (Optional) If additional processing is needed, create a temporary view and run further SQL queries
staged_streaming_df.createOrReplaceTempView("staged_streaming")
final_aggregated = spark.sql("""
    SELECT customer_id, 
           total_sales
    FROM staged_streaming
    ORDER BY total_sales DESC
""")
display(final_aggregated)

# Write the final aggregated streaming data to the final Delta Lake table (data warehouse)
final_delta_path = "/mnt/delta/streaming_warehouse_sales"
final_aggregated.write.format("delta") \
    .mode("overwrite") \
    .save(final_delta_path)

# Optionally, register the final Delta table in Spark SQL for interactive querying
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS streaming_warehouse_sales
    USING DELTA
    LOCATION '{final_delta_path}'
""")
```
---

## Execution Order in Your Job

1. **Notebook 1:** Batch Data Ingestion & Save as Delta/Parquet  
   (Ingest raw CSV data and persist as Delta.)

2. **Notebook 2:** Batch Data Transformation & Enrichment  
   (Read persisted batch data, transform and join datasets, then save enriched data.)

3. **Notebook 3:** Batch Data Aggregation & Final Load  
   (Read enriched batch data, aggregate it, and write the final warehouse table.)

4. **Notebook 4:** Streaming Data Ingestion & Transformation  
   (Continuously ingest and transform streaming data, writing output to a staging Delta table. Run for a predetermined duration if needed.)

5. **Notebook 5:** Streaming Data Finalization & Write to Data Warehouse  
   (Read the staged streaming results and write the final streaming warehouse table.)

