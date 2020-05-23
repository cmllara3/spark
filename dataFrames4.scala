// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Intro To DataFrames, Lab #4
// MAGIC ## What-The-Monday?

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC The datasource for this lab is located on the DBFS at **/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet**.
// MAGIC 
// MAGIC As we saw in the previous notebook...
// MAGIC * There are a lot more requests for sites on Monday than on any other day of the week.
// MAGIC * The variance is **NOT** unique to the mobile or desktop site.
// MAGIC 
// MAGIC Your mission, should you choose to accept it, is to demonstrate conclusively why there are more requests on Monday than on any other day of the week.
// MAGIC 
// MAGIC Feel free to copy & paste from the previous notebook.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// TODO
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val fileName = "/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet"

val dfInicial = spark.read
  .parquet(fileName)
  .withColumn("timestamp", unix_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp"))

dfInicial.printSchema()
println("Cantidad de Registros = "+dfInicial.count())
println("Cantidad de Requests = "+dfInicial.agg(sum("requests")).first.get(0))

// COMMAND ----------

val groupDF = dfInicial
  .groupBy(date_format($"timestamp","yyyy").as("Year"),date_format($"timestamp","MMM").as("Month"),date_format($"timestamp","u-E").as("Day of Week"),$"site")
  .sum("requests")
  .orderBy($"Month",$"Day Of Week")
  .withColumnRenamed("sum(requests)", "Total Requests")
  .withColumnRenamed("site", "Site")
  
groupDF.printSchema()
println("Cantidad de Resgistros = "+groupDF.count())
println("Cantidad de Requests = "+groupDF.agg(sum("Total Requests")).first.get(0))

// COMMAND ----------

//La diferencia con respecto al dia de la semana se presenta por solicitudes en computadoras, el comportamiento de los mobiles es mas estatico
display(groupDF)

// COMMAND ----------

//En terminos generales sin importar el dia las solicitudes que se realizan en desktop son mucho mayor
display(groupDF)

// COMMAND ----------

val groupDFDesktop = groupDF
  .filter($"Site" === "desktop")

groupDFDesktop.printSchema()

// COMMAND ----------

//El comportamiento en marzo de las consulta al sitio por dia se pueden considerar estables, mientras que para abril maneja picos en ciertos dias
display(groupDFDesktop)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
