# Databricks notebook source
# from delta.tables import DeltaTable
# primarykeys=['enrollment_id']
# cols=['student_id','course_id','enrollment_date']
# silver_path='abfss://silver@udemydatalakestorage01.dfs.core.windows.net/enrollment'
# table_exists=DeltaTable.isDeltaTable(spark,silver_path)

# COMMAND ----------

# if not table_exists:
#     df_enroll.write.format('delta').mode('overwrite').save(silver_path)
# else:
#     tgt_table=DeltaTable.forPath(spark,silver_path)
#     upd=' OR '.join([f'src.{col}<>tgt.{col}' for col in cols])
#     tgt_table.alias('tgt').merge(
#         source=df_enroll.alias('src'),
#         condition=' AND '.join([f'tgt.{k}=src.{k}' for k in primarykeys])+' AND tgt.is_current=True'
#     ).whenMatchedUpdate(
#         condition=upd,
#         set={
#             'is_current':'False'
#         }
#     ).whenNotMatchedInsert(
#         values={
#             **{col:f'src.{col}' for col in df_enroll.columns}
#             }
#     )

# COMMAND ----------

# Databricks notebook for processing incremental Enrollment data

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from pyspark.sql.functions import (
    col, coalesce, lit, when, to_date, to_timestamp,
    input_file_name, split, element_at,
    monotonically_increasing_id, row_number, max as sql_max,
    first, count, current_timestamp
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# COMMAND ----------

bronze_path = "abfss://bronze@udemydatalakestorage001.dfs.core.windows.net/enrollment"
silver_path = "abfss://silver@udemydatalakestorage001.dfs.core.windows.net/Enrollment"
processed_files_table = "udemy.silver.processed_files_enrollment"

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

enrollment_schema = StructType([
    StructField("enrollment_id", IntegerType(), True),
    StructField("student_id", IntegerType(), True),
    StructField("course_id", IntegerType(), True),
    StructField("enrollment_date", StringType(), True)
])


# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {processed_files_table} (
  filename STRING NOT NULL,
  input_file STRING NOT NULL,
  processed_at TIMESTAMP NOT NULL,
  file_size BIGINT,
  records_processed BIGINT NOT NULL,
  CONSTRAINT pk_processed_enrollment PRIMARY KEY (filename)
) USING DELTA
""")

# COMMAND ----------


def get_new_files():
    df_all_files = (
        spark.read.format("csv")
        .option("header", True)
        .schema(enrollment_schema)
        .load(bronze_path)
        .withColumn("input_file", input_file_name())
        .withColumn("filename", element_at(split(input_file_name(), "/"), -1))
        .select("filename").distinct()
    )
    processed = [row.filename for row in spark.table(processed_files_table).select("filename").collect()]
    return df_all_files.filter(~col("filename").isin(processed))

# COMMAND ----------


def process_enrollment_files(df_new_files):
    if df_new_files.count() == 0:
        print("No new files to process.")
        return None

    filenames = [row.filename for row in df_new_files.collect()]

    df = (
        spark.read.format("csv")
        .option("header", True)
        .schema(enrollment_schema)
        .load(bronze_path)
        .withColumn("input_file", input_file_name())
        .withColumn("filename", element_at(split(input_file_name(), "/"), -1))
        .filter(col("filename").isin(filenames))
    )

    df = df.dropDuplicates(["student_id", "course_id"])

    df = df.withColumn("enrollment_date",
    when(col("enrollment_date").rlike("^\\d{4}-\\d{2}-\\d{2}$"),
         to_date(col("enrollment_date"), "yyyy-MM-dd"))
    .when(col("enrollment_date").rlike("^\\d{2}/\\d{2}/\\d{4}$"),
         to_date(col("enrollment_date"), "dd/MM/yyyy"))
    .when(col("enrollment_date").rlike("^\\d{2}-\\d{2}-\\d{4}$"),  
         to_date(col("enrollment_date"), "dd-MM-yyyy"))
    .otherwise(lit(None))
    )

    df = df.withColumn("is_current", lit(True))
    (df.write
     .format("delta")
     .mode("append")
     .option("mergeSchema", "true")
     .save(silver_path))

    df_metadata = (
        df.groupBy("filename")
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
metadata = process_enrollment_files(new_files)
if metadata is not None:
    metadata.write.format("delta").mode("append").saveAsTable(processed_files_table)
    print(f"✅ Successfully processed {metadata.count()} new files.")
    display(metadata)
else:
    print("No new files to process.")

# COMMAND ----------

df_all_files = (
        spark.read.format("csv")
        .option("header", True)
        .schema(enrollment_schema)
        .load(bronze_path)
    )
df_all_files.count()
df_all_files.filter(col('student_id')==1501).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS udemy.silver.enrollment
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@udemydatalakestorage001.dfs.core.windows.net/Enrollment';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM udemy.silver.enrollment ;

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM udemy.silver.enrollment").show()