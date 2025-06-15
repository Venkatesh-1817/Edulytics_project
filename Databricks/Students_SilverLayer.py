# Databricks notebook source
# MAGIC %md
# MAGIC ### Loading Data

# COMMAND ----------

# MAGIC %md
# MAGIC Student Files

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("signup_date", StringType(), True), 
    StructField("location", StringType(), True)
])

# COMMAND ----------

from pyspark.sql.functions import *
df_student=spark.read.format('csv')\
    .option('header','True')\
    .schema(schema)\
    .load('abfss://bronze@udemydatalakestorage001.dfs.core.windows.net/student')

# COMMAND ----------

df_student.display()

# COMMAND ----------

df_student.drop('_rescued_data')

# COMMAND ----------

df_student=df_student.dropDuplicates(['student_id'])

# COMMAND ----------

df_student.printSchema()

# COMMAND ----------

df_student = df_student.withColumn(
    "signup_date",
    when(col("signup_date").rlike(r"^\d{4}-\d{2}-\d{2}$"), to_date("signup_date", "yyyy-MM-dd"))
    .when(col("signup_date").rlike(r"^\d{2}-\d{2}-\d{4}$"), to_date("signup_date", "dd-MM-yyyy"))
    .when(col("signup_date").rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"), to_date("signup_date", "d/M/yyyy"))
    .otherwise(None)
)

# COMMAND ----------

df_student.count()

# COMMAND ----------

from delta.tables import DeltaTable
df_scd = df_student.withColumn("start_date", current_timestamp()) \
           .withColumn("end_date", lit("9999-12-31").cast("timestamp")) \
           .withColumn("is_current", lit(True))

# COMMAND ----------

df_scd.display()

# COMMAND ----------

primary_keys = ["student_id"]
compare_cols = ["name", "email", "location", "signup_date"]
silver_path='abfss://silver@udemydatalakestorage001.dfs.core.windows.net/Students'
table_exists = DeltaTable.isDeltaTable(spark, silver_path)

# COMMAND ----------

if not table_exists:
    df_scd.write.format("delta").mode("overwrite").save(silver_path)
else:
    target = DeltaTable.forPath(spark, silver_path)
    updates_expr = " OR ".join([f"source.{col} <> target.{col}" for col in compare_cols])
    target.alias("target").merge(
        source=df_scd.alias("source"),
        condition=" AND ".join([f"source.{k} = target.{k}" for k in primary_keys]) + " AND target.is_current = True"
    ).whenMatchedUpdate(
        condition=updates_expr,
        set={
            "end_date": "current_timestamp()",
            "is_current": "false"
        }
    ).whenNotMatchedInsert(
        values={
            **{col: f"source.{col}" for col in df_scd.columns}
        }
    ).execute()

# COMMAND ----------

spark.read.format('delta')\
    .option('header','True')\
    .load('abfss://silver@udemydatalakestorage001.dfs.core.windows.net/Students').display()