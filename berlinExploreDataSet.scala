// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC 1.- Download the data for the Berlin Airbnb  in https://www.kaggle.com/brittabettendorf/berlin-airbnb-data
// MAGIC 
// MAGIC 2.- Upload the CSVs to Databricks:
// MAGIC 
// MAGIC - calendar_summary.csv
// MAGIC - listings.csv
// MAGIC - listings_summary.csv
// MAGIC - neighbourhoods.csv
// MAGIC - reviews.csv
// MAGIC - reviews_summary.csv
// MAGIC     
// MAGIC 3.- Solve below questions for "calendar_summary.csv"
// MAGIC 
// MAGIC - listing_id: House ID
// MAGIC - date: Date
// MAGIC - available: If the apartment is available for the date false/true
// MAGIC - price: for renting the flat that date

// COMMAND ----------

// DBTITLE 1,Probar que los CSVs se han subido correctamente
//Esto no es un ejercicio, es sólo para probar que se han subido correctamente los CSVs
dbutils.fs.ls("dbfs:/FileStore/tables").foreach(println(_))

// COMMAND ----------

// DBTITLE 1,show() - Mostrar dataframe "calendar_summary.csv" por consola
//<Fill-here>
val calendar_summary_csv = "dbfs:/FileStore/tables/calendar_summary.csv/"

val df = spark// Our SparkSession & Entry Point
  .read// Our DataFrameReader
  .option("header", "true")//Se toma la opcion del header        
  .option("inferSchema", "true")//Se infiere el esquema de la tabla
  .csv(calendar_summary_csv)//Se lee el archivo csv

df.show()//Se muestra en consola el DF

// COMMAND ----------

// DBTITLE 1,display(..)- Mostrar dataframe "calendar_summary.csv" usando display()
//<Fill-here>
display(df)//Se muestra bonito el DF

// COMMAND ----------

// DBTITLE 1,Mostrar sólo 3 líneas en consola
//<Fill-here>
df.show(3)//Se muestra solo los 3 primeros registros

// COMMAND ----------

// DBTITLE 1,Mostrar en consola sólo las columnas "date" y "available"
//<Fill-here>
val dfDateAvailable = df
  .select("date","available")//Se seleccionan las columnas "date" y "available"

dfDateAvailable.show()//Se muestra en consola el DF

// COMMAND ----------

// DBTITLE 1,Mostrar la cantidad registros que tiene la tabla
//<Fill-here>
df.count()//Se cuenta la cantidad de registros del DF

// COMMAND ----------

// DBTITLE 1,Mostrar por consola las viviendas diferentes
//<Fill-here>
val dfViviendaDiferentes = df
  .select("listing_id")//Se selecciona la columna "listing_id"
  .distinct()//Se ponen registros unicos

dfViviendaDiferentes.show()//Se muestra el DF en consola

// COMMAND ----------

// DBTITLE 1,¿Cuántas viviendas diferentes oferta airbnb en Berlin?
//<Fill-here>
dfViviendaDiferentes.count()//Se cuenta la cantidad de viviendas diferentes
