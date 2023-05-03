// Databricks notebook source
// MAGIC %sql show tables in default

// COMMAND ----------

spark.sparkContext.getPersistentRDDs

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from default.scrubbed limit 1

// COMMAND ----------

spark.sqlContext.cacheTable("default.scrubbed")

// COMMAND ----------

spark.sparkContext.getPersistentRDDs

// COMMAND ----------

// MAGIC %sql select * from default.scrubbed limit 1

// COMMAND ----------

spark.sparkContext.getPersistentRDDs

// COMMAND ----------

spark.sqlContext.clearCache

// COMMAND ----------

spark.sparkContext.getPersistentRDDs

// COMMAND ----------

val scrubbed_df=spark.read.table("default.scrubbed")

// COMMAND ----------

spark.sparkContext.getPersistentRDDs

// COMMAND ----------

scrubbed_df.createOrReplaceTempView("scrubbed_view")

// COMMAND ----------

spark.sparkContext.getPersistentRDDs

// COMMAND ----------

scrubbed_df.cache.createOrReplaceTempView("scrubbed_view")

// COMMAND ----------

spark.sparkContext.getPersistentRDDs

// COMMAND ----------

// MAGIC %sql select * from scrubbed_view limit 1

// COMMAND ----------

spark.sparkContext.getPersistentRDDs

// COMMAND ----------

// MAGIC %sql select * from scrubbed_view limit 1

// COMMAND ----------

spark.sqlContext.clearCache

// COMMAND ----------

// MAGIC %sql
// MAGIC CACHE TABLE scrubbed_cache OPTIONS ('storageLevel' 'DISK_ONLY') select * from default.scrubbed

// COMMAND ----------

spark.sparkContext.getPersistentRDDs

// COMMAND ----------

// MAGIC %sql select * from scrubbed_cache

// COMMAND ----------

// MAGIC %sql
// MAGIC UNCACHE TABLE IF EXISTS scrubbed_cache

// COMMAND ----------

// MAGIC %sql select * from scrubbed_cache

// COMMAND ----------

spark.sparkContext.getPersistentRDDs

// COMMAND ----------

val scrubbed_df=spark.read.table("default.scrubbed")

// COMMAND ----------

scrubbed_df.persist()

// COMMAND ----------

scrubbed_df.count()

// COMMAND ----------

scrubbed_df.unpersist()

// COMMAND ----------

spark.sparkContext.getPersistentRDDs

// COMMAND ----------

scrubbed_df.count()

// COMMAND ----------


