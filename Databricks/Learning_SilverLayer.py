# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# COMMAND ----------

bronze_path = "abfss://bronze@udemydatalakestorage001.dfs.core.windows.net/learning"
silver_path = "abfss://silver@udemydatalakestorage001.dfs.core.windows.net/Learning"
processed_files_table = "udemy.silver.processed_files_tracking"

# COMMAND ----------

# Enable schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
bronze_schema = StructType([
    StructField('log_id', IntegerType(), True),
    StructField('student_id', IntegerType(), True),
    StructField('course_id', IntegerType(), True),
    StructField('activity_type', StringType(), True),
    StructField('timestamp', StringType(), True),
    StructField('duration_min', IntegerType(), True)
])


# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {processed_files_table} (
  filename STRING NOT NULL,
  input_file STRING NOT NULL,
  processed_at TIMESTAMP NOT NULL,
  file_size BIGINT,
  records_processed BIGINT NOT NULL,
  CONSTRAINT pk_processed_files PRIMARY KEY (filename)
) USING DELTA
""")

# COMMAND ----------

def get_new_files():
    """Get list of files in Bronze that haven't been processed yet"""
    df_all_files = (spark.read.format("csv")
                   .option("header", True)
                   .schema(bronze_schema)
                   .load(bronze_path)
                   .withColumn("input_file", input_file_name())
                   .withColumn("filename", element_at(split(col("input_file"), "/"), -1))
                   .select("filename").distinct())
    processed_files = [row.filename for row in spark.table(processed_files_table).select("filename").collect()]
    
    return df_all_files.filter(~col("filename").isin(processed_files))


# COMMAND ----------

from pyspark.sql.functions import (
    col, split, element_at, input_file_name, coalesce, lit, when,
    to_timestamp, monotonically_increasing_id, row_number, current_timestamp,
    first, count
)
from pyspark.sql.types import LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import max as sql_max

def process_incremental_data(df_new_files):
    """Process new files and return metadata"""
    if df_new_files.count() == 0:
        print("No new files to process.")
        return None

    df_new_data = (
        spark.read.format("csv")
        .option("header", True)
        .schema(bronze_schema)
        .load(bronze_path)
        .withColumn("input_file", input_file_name())
        .withColumn("filename", element_at(split(col("input_file"), "/"), -1))
        .filter(col("filename").isin([row.filename for row in df_new_files.collect()]))
    )

    df_deduped = df_new_data.dropDuplicates([
        "student_id", "course_id", "activity_type", 
        "timestamp", "duration_min", "filename"
    ])

    df_clean = (
        df_deduped
        .withColumn("duration_min", coalesce(col("duration_min").cast("int"), lit(0)))
        .withColumn("activity_type", coalesce(col("activity_type"), lit("unknown")))
    )

    df_with_ts = (
        df_clean
        .withColumnRenamed("timestamp", "ts_raw")
        .withColumn("ts_trimmed", col("ts_raw").cast("string"))
        .withColumn(
            "timestamp",
            when(col("ts_trimmed").rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"),
                 to_timestamp(col("ts_trimmed"), "yyyy-MM-dd HH:mm:ss"))
            .when(col("ts_trimmed").rlike(r"^\d{1,2}-\d{1,2}-\d{4} \d{2}:\d{2}:\d{2}$"),
                 to_timestamp(col("ts_trimmed"), "d-M-yyyy HH:mm:ss"))
            .when(col("ts_trimmed").rlike(r"^\d{1,2}-\d{1,2}-\d{4} \d{2}:\d{2}$"),
                 to_timestamp(col("ts_trimmed"), "d-M-yyyy HH:mm"))
            .when(col("ts_trimmed").rlike(r"^\d{1,2}/\d{1,2}/\d{4} \d{2}:\d{2}$"),
                 to_timestamp(col("ts_trimmed"), "d/M/yyyy HH:mm"))
            .otherwise(lit(None))
        )
        .drop("ts_raw", "ts_trimmed")
    )

    try:
        df_existing = spark.read.format("delta").load(silver_path)
        max_id = df_existing.agg(sql_max("log_id")).collect()[0][0] or 0
        ts_type = df_existing.schema["timestamp"].dataType
        df_with_ts = df_with_ts.withColumn("timestamp", col("timestamp").cast(ts_type))
    except:
        max_id = 0

    window = Window.orderBy(monotonically_increasing_id())
    df_final = (
        df_with_ts
        .withColumn("rn", row_number().over(window))
        .withColumn("log_id", col("rn") + max_id)
        .drop("rn")
    )

    (
        df_final.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(silver_path)
    )
    df_metadata = (
        df_final.groupBy("filename")
        .agg(
            first("input_file").alias("input_file"),
            current_timestamp().alias("processed_at"),
            lit(0).cast(LongType()).alias("file_size"),
            count(lit(1)).alias("records_processed")
        )
    )

    return df_metadata


# COMMAND ----------

new_files = get_new_files()
processing_metadata = process_incremental_data(new_files)

if processing_metadata is not None:
    (processing_metadata.write
     .format("delta")
     .mode("append")
     .option("mergeSchema", "true")
     .saveAsTable(processed_files_table))
    
    print(f"Successfully processed {processing_metadata.count()} new files.")
    display(processing_metadata)
else:
    print("No new files to process.")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists udemy.silver.learning using 
# MAGIC delta location 'abfss://silver@udemydatalakestorage001.dfs.core.windows.net/Learning';

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from udemy.silver.learning;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from udemy.silver.learning;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from udemy.silver.processed_files_tracking;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table udemy.silver.processed_files_tracking;

# COMMAND ----------

