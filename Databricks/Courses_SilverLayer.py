# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *
schema=StructType([StructField("course_id",IntegerType(),True),
                   StructField("course_name",StringType(),True),
                   StructField("category",StringType(),True),
                   StructField('instructor',StringType(),True),
                   StructField('launch_date',StringType(),True),
                   StructField('course_duration',IntegerType(),True)
                   ])

# COMMAND ----------

df_cor=spark.read.format('csv')\
    .option('header','true')\
    .schema(schema)\
    .load('abfss://bronze@udemydatalakestorage001.dfs.core.windows.net/course')
df_cor.display()

# COMMAND ----------

df_cor.printSchema()

# COMMAND ----------

df_cor.count()

# COMMAND ----------

df_cor.createOrReplaceTempView('courses')

# COMMAND ----------

spark.sql('select category,count(*) from courses group by category').display()

# COMMAND ----------

spark.sql('select instructor,count(*) from courses group by instructor order by count(*) desc').display()

# COMMAND ----------

#course with max duration 
spark.sql('select * from courses where course_duration=(select max(course_duration) from courses)').display()

# COMMAND ----------

df_cor=df_cor.withColumn('launch_date',to_date(col('launch_date'),'yyyy-MM-dd'))

# COMMAND ----------

df_cor=df_cor.withColumn('is_current',lit(True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type2

# COMMAND ----------

from delta.tables import DeltaTable
primary_keys=['course_id']
cols=['course_name','category','instructor','launch_date','course_duration']
silver_path='abfss://silver@udemydatalakestorage001.dfs.core.windows.net/courses'
table_exists=DeltaTable.isDeltaTable(spark,silver_path)

# COMMAND ----------

if not table_exists:
    df_cor.write.format('delta').mode('overwrite').save(silver_path)
else:
    tgt_table=DeltaTable.forPath(spark,silver_path)
    upd_exp=' OR '.join([f'src.{col}<>tgt.{col}' for col in cols])
    tgt_table.alias('tgt').merge(
        source=df_cor.alias('src'),
        condition=' AND '.join([f'src.{k}=tgt.{k}' for k in primary_keys])+" AND tgt.is_current=True"
    ).whenMatchedUpdate(condition=upd_exp,
                        set={
                        'is_current':'False'
            }
    ).whenNotMatchedInsert(
values={
    **{col: f'src.{col}' for col in df_cor.columns}
}
            )

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists udemy.silver.courses using
# MAGIC delta location 'abfss://silver@udemydatalakestorage001.dfs.core.windows.net/courses';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from udemy.silver.courses;