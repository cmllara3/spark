// Databricks notebook source
// DBTITLE 1,Ejercicio con el Dataset de Berlin
// MAGIC %md
// MAGIC Vamos a trabajar con el DataSet de airbnb de Berlin y se dará un tiempo para completar cada ejercicio. Cuando el tiempo haya terminado daremos la solución todos juntos.

// COMMAND ----------

// DBTITLE 1,Ficheros que se necesitan
dbutils.fs.ls("dbfs:/FileStore/tables").foreach(println(_))

// COMMAND ----------

// DBTITLE 1,Mostrar por pantalla - calendar_summary.csv
import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._

val calendar_summary_csv = "dbfs:/FileStore/tables/calendar_summary.csv/"

val df = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")
  .csv(calendar_summary_csv)//Se lee el archivo csv
  
display(df)

// COMMAND ----------

df
  .repartition(8)
  .cache()     

// COMMAND ----------

// DBTITLE 1,Disponibilidad de viviendas diaria airbnb en Berlin - Mostrar en gráfico
val df_only_available_days = df
  .groupBy(date_format($"date","yyyy-MM-dd").as("date"),$"available")
  .count()
  .orderBy($"date")

display(df_only_available_days)

// COMMAND ----------

// DBTITLE 1,Disponibilidad de viviendas mensual airbnb en Berlin - Mostrar en gráfico
val df_only_available_days = df
  .groupBy(date_format($"date","yyyy, MM").as("date"),$"available")
  .count()
  .orderBy($"date")

display(df_only_available_days)

// COMMAND ----------

display(dfNew)

// COMMAND ----------

// DBTITLE 1,Precio medio por día - Mostrar en gráfico
val df_only_available_days = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")
  .csv(calendar_summary_csv)//Se lee el archivo csv
  .withColumn("price_double",regexp_replace(col("price"),"\\$","").cast(DoubleType))//Cast como numerico
  .groupBy(date_format($"date","yyyy-MM-dd").as("date"))//Formato de fecha
  .orderBy($"date")//Ordena por fecha

display(df_only_available_days)

// COMMAND ----------

// DBTITLE 1,Oferta de vivienda por barrio - Mostrar en gráfico
val csv_path = "dbfs:/FileStore/tables/listings.csv"

val df = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")
  .csv(csv_path)//Se lee el archivo csv

display(df)

// COMMAND ----------

// DBTITLE 1,Barrios con menor disponibilidad en el año - Mostrar en gráfico 
val csv_path = "dbfs:/FileStore/tables/listings.csv"

val df = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")
  .csv(csv_path)//Se lee el archivo csv

val dfGrouped = df
  .groupBy($"neighbourhood_group")
  .avg("availability_365")

display(dfGrouped)

// COMMAND ----------

// DBTITLE 1,Nombre + Comentarios de la vivienda con listing_id = "2015" 
val csv_listing = "dbfs:/FileStore/tables/listings.csv"

val csv_coments = "dbfs:/FileStore/tables/reviews_summary.csv"

val df_house_name = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")
  .csv(csv_listing)//Se lee el archivo csv
  .select($"id",$"name")
  .filter($"id" === 2015)

val df_comments = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")
  .csv(csv_coments)//Se lee el archivo csv
  .select($"listing_id",$"date",$"comments")
  .filter($"listing_id" === 2015)

val df_join = df_house_name
  .join(df_comments,df_house_name("id") === df_comments("listing_id"))
  .select($"id",$"name",$"date",$"comments")

display(df_join)

// COMMAND ----------

// DBTITLE 1,10 Personas con más comentarios
import org.apache.spark.sql.functions._


val df_comments = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")
  .csv(csv_coments)//Se lee el archivo csv
  .filter($"reviewer_name" =!= "null")
  .groupBy($"reviewer_name")
  .count()
  .orderBy(desc("count"))
  .head(10)
  
display(df_comments)

// COMMAND ----------

// DBTITLE 1,Comentarios con nombre de apartamento de la persona con reviewer_id = "76365146"
val csv_listing = "dbfs:/FileStore/tables/listings.csv"

val csv_coments = "dbfs:/FileStore/tables/reviews_summary.csv"

val df_house_name = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")
  .csv(csv_listing)//Se lee el archivo csv
  .select($"id",$"host_id",$"host_name",$"name",$"room_type")
  
val df_comments = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")
  .csv(csv_coments)//Se lee el archivo csv
  .select($"listing_id",$"reviewer_id",$"reviewer_name",$"date",$"comments")
  .filter($"reviewer_id" === 76365146)

val df_join = df_house_name
  .join(df_comments,df_house_name("id") === df_comments("listing_id"))
  .select($"id",$"host_id",$"host_name",$"name",$"room_type",$"reviewer_id",$"reviewer_name",$"date",$"comments")

display(df_join)

// COMMAND ----------

// DBTITLE 1,Personas que se auto-comentan
val csv_listing = "dbfs:/FileStore/tables/listings.csv"

val csv_coments = "dbfs:/FileStore/tables/reviews_summary.csv"

val df_house_name = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")
  .csv(csv_listing)//Se lee el archivo csv
  .select($"host_id",$"host_name")
  
val df_comments = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")
  .csv(csv_coments)//Se lee el archivo csv
  .select($"reviewer_id",$"reviewer_name",$"date",$"comments")

val df_join = df_house_name
  .join(df_comments,df_house_name("host_id") === df_comments("reviewer_id"))
  .select($"host_id",$"host_name",$"reviewer_id",$"reviewer_name",$"date",$"comments")

display(df_join)

// COMMAND ----------

// DBTITLE 1,Distancia al centro de Berlin
import org.apache.spark.sql.functions._

case class Location(lat: Double, lon: Double)
trait DistanceCalcular {
    def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int
}
class DistanceCalculatorImpl extends DistanceCalcular {
    private val AVERAGE_RADIUS_OF_EARTH_KM = 6371
    override def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int = {
        val latDistance = Math.toRadians(userLocation.lat - warehouseLocation.lat)
        val lngDistance = Math.toRadians(userLocation.lon - warehouseLocation.lon)
        val sinLat = Math.sin(latDistance / 2)
        val sinLng = Math.sin(lngDistance / 2)
        val a = sinLat * sinLat +
        (Math.cos(Math.toRadians(userLocation.lat)) *
            Math.cos(Math.toRadians(warehouseLocation.lat)) *
            sinLng * sinLng)
        val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
        (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
    }
}
new DistanceCalculatorImpl().calculateDistanceInKilometer(Location(10, 20), Location(40, 20))



val berlin_centre = Location(52.520008, 13.404954)


spark.udf.register("havershine", (lat: Double, lon:Double) => new DistanceCalculatorImpl().calculateDistanceInKilometer(Location(lat, lon), berlin_centre))

val df = spark.read.option("header", "true").csv("dbfs:/FileStore/tables/listings.csv")//<<Fill in>>


display(df)


// COMMAND ----------


