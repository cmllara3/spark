// Databricks notebook source
// MAGIC %md
// MAGIC ##![Mongo_logo](https://cloud.mongodb.com/static/images/favicon.ico)Conectar databricks a nuestra MongoDB I
// MAGIC 
// MAGIC Configure Databricks Cluster with MongoDB Connection URI
// MAGIC 
// MAGIC 1. Abrimos MongoDB Cloud -> [Mongo Cloud](https://cloud.mongodb.com/)
// MAGIC 2. Click en Connect.
// MAGIC 3. Click Connect Your Application.
// MAGIC 4. Seleccionar Scala en el desplegable y la útlima versión
// MAGIC 5. Click the SRV connection string tile y copia la connection string. Debería ser algo como: ***mongodb+srv://XXXXX:YYYYYY@<cluster-name>wlcof.azure.mongodb.net/test
// MAGIC 6. Crea un nuevo usuario.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) **Configurar MongoDB en el cluster** II
// MAGIC - Click en clusters
// MAGIC - Click en el cluster que esté running
// MAGIC - Click en edit
// MAGIC - En la parte de abajo de la pantalla, debería aparecer una tab que pone spark, click ahí. 
// MAGIC - Pega lo siguiente y restart the cluster
// MAGIC   - spark.mongodb.input.uri mongodb+srv://XXX:YYY@cluster0-pgjau.gcp.mongodb.net/test
// MAGIC   - spark.mongodb.output.uri mongodb+srv://XXX:YYY@cluster0-pgjau.gcp.mongodb.net/test

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Python Logo Tiny](http://icons.iconarchive.com/icons/cornmanthe3rd/plex/32/Other-python-icon.png) Instalar las librerias de python
// MAGIC 
// MAGIC ####![warning Logo Tiny](http://icons.iconarchive.com/icons/martz90/hex/32/warning-icon.png) Es necesario ejecutarlo cada vez que arranquemos el cluster

// COMMAND ----------

// MAGIC %sh
// MAGIC pip install --upgrade pip
// MAGIC pip install tweepy

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![twitter Logo Tiny](http://icons.iconarchive.com/icons/limav/flat-gradient-social/32/Twitter-icon.png) Descarguemos algunos twits!
// MAGIC 
// MAGIC 
// MAGIC ####![warning Logo Tiny](http://icons.iconarchive.com/icons/martz90/hex/32/warning-icon.png)Es necesario tener una app creada en [twitter devs](https://developer.twitter.com/en). Sin esto no podremos realizar el entregable. 
// MAGIC ####![warning Logo Tiny](http://icons.iconarchive.com/icons/martz90/hex/32/warning-icon.png)Solicitar la cuenta lleva unos días
// MAGIC 
// MAGIC ####![twitter Logo Tiny](http://icons.iconarchive.com/icons/limav/flat-gradient-social/32/Twitter-icon.png) Una vez tengamos la cuenta creada hay que crear una APP
// MAGIC ####![twitter Logo Tiny](http://icons.iconarchive.com/icons/limav/flat-gradient-social/32/Twitter-icon.png) Cuando tengamos la app, en los detalles de la cuenta veremos una tab que pone keys and secrets
// MAGIC 
// MAGIC |Nombre en twitter|Nombre en el codigo|Codigos|
// MAGIC |-------------|:-------------:| -----:|
// MAGIC | API key      | consumer_key | XXXX |
// MAGIC | API secret key     | consumer_secret      |XXXX|
// MAGIC | Access token | access_token     |  XXXX |
// MAGIC | Access token secret| access_token_secret     |    XXX |
// MAGIC 
// MAGIC Los datos de arriba son de ejemplo no van a funcionar
// MAGIC 
// MAGIC ![Ayuda](http://icons.iconarchive.com/icons/oxygen-icons.org/oxygen/32/Actions-help-about-icon.png)[![AYUDA!!!](https://i.ytimg.com/vi/KPHC2ygBak4/hqdefault.jpg?sqp=-oaymwEZCNACELwBSFXyq4qpAwsIARUAAIhCGAFwAQ==&rs=AOn4CLBWH95Z7t7-terVqhcvpvKWbeKI1w)](https://www.youtube.com/watch?v=KPHC2ygBak4) Podéis encontrar algo de ayuda en el vídeo
// MAGIC 
// MAGIC ***Pasados unos minutos, podemos parar la captura de twits***

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Streaming de Twitter

// COMMAND ----------

// MAGIC %md
// MAGIC Borrar los archivos que estan actualmente en la carpeta: <b><i>dbfs:///twitterdata/</i></b>

// COMMAND ----------

dbutils.fs.rm("dbfs:///twitterdata/",true)

val cantidad = dbutils.fs.ls("dbfs:///").toDF().filter($"name" === "twitterdata/").count()

if (cantidad == 0)
{
    dbutils.fs.mkdirs("dbfs:///twitterdata/")
}

// COMMAND ----------

// MAGIC %python
// MAGIC from __future__ import absolute_import, print_function
// MAGIC from tweepy import OAuthHandler, Stream, StreamListener
// MAGIC from tweepy.streaming import StreamListener
// MAGIC import json
// MAGIC 
// MAGIC consumer_key = "XXX"
// MAGIC consumer_secret = "XXX"
// MAGIC access_token = "XXX"
// MAGIC access_token_secret = "XXX"
// MAGIC 
// MAGIC class TwitterListener(StreamListener):
// MAGIC   
// MAGIC     def __init__(self):
// MAGIC         super().__init__()
// MAGIC         self.counter = 0
// MAGIC         self.limit = 8113 #Cantidad a descarga
// MAGIC 
// MAGIC     def on_data(self,data):
// MAGIC         try:
// MAGIC               dictData = json.loads(data)
// MAGIC               dictData["id"] = str(dictData["id"])
// MAGIC               print("Writing data to: /dbfs/twitterdata/"+dictData["id"]+".json")     
// MAGIC               # COMPLETAR CON LA ESCRITURA DE LOS FICHEROS AL DBFS. 
// MAGIC               # COMO PISTA: dictData tiene que pasarse a JSON usando json.dumps
// MAGIC               with open("/dbfs/twitterdata/"+dictData["id"]+".json", 'w') as outfile:
// MAGIC                 j = json.dumps(dictData)
// MAGIC                 outfile.write(j)
// MAGIC               self.counter += 1
// MAGIC               if self.counter < self.limit:
// MAGIC                 print("Tweet número = "+str(self.counter+1))
// MAGIC                 return True
// MAGIC               else:
// MAGIC                 stream.disconnect()
// MAGIC               
// MAGIC         except BaseException as e:
// MAGIC             print("Error on_data: %s" % str(e))
// MAGIC         return True
// MAGIC 
// MAGIC     def on_error(self, status):
// MAGIC         print(status)
// MAGIC 
// MAGIC if __name__ == '__main__':
// MAGIC     l = TwitterListener()
// MAGIC     auth = OAuthHandler(consumer_key, consumer_secret)
// MAGIC     auth.set_access_token(access_token, access_token_secret)
// MAGIC     stream = Stream(auth, l)
// MAGIC     stream.filter(track=['COVID @INFOPRESIDENCIA','COVID @CANCILLERIACOL','COVID @SENADOGOVCO','COVID @ALCALDIACTG','COVID @VICECOLOMBIA','COVID @MININTERIOR','COVID @ALCALDIAIBAGUE','COVID @MINTRABAJOCOL',
// MAGIC                          'COVID @ALCALDIACUCUTA','COVID @MINSALUDCOL','COVID @SFCSUPERVISOR','COVID @BOGOTA','COVID @DANE_COLOMBIA','COVID @ALCALDIADEMED','COVID @ALCALDIADECALI','COVID @FISCALIACOL',
// MAGIC                          'COVID @ALCALDIABQUILLA'],languages =["es"]) #añadir hashtags separados por ','

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Descarga de los Tweets por paquetes

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC import tweepy
// MAGIC import json
// MAGIC from tweepy.parsers import JSONParser
// MAGIC 
// MAGIC #Parametros de acceso a Twitter
// MAGIC consumer_key = "RXIDBuApcmOrISMziPi2hpuEP"
// MAGIC consumer_secret = "l5hWWcFG8gRBjbmpwGJdRoXKnVafnXdpqFTx7B4F8p737AAFHm"
// MAGIC access_token = "128621028-7UasDFuekLYgRfgWo1bq3tGPJZBz3YNk3XqqpLIR"
// MAGIC access_token_secret = "L93jMDDPEY0KSLzqCO43mbNKXWRKOA5wbedkV0B69V28h"
// MAGIC 
// MAGIC #Conectar con Twitter
// MAGIC auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
// MAGIC auth.set_access_token(access_token, access_token_secret)
// MAGIC api = tweepy.API(auth,parser=tweepy.parsers.JSONParser())
// MAGIC 
// MAGIC #Variable de proceso para identificar el ultimo Twitter descargado
// MAGIC menor = 9999999999999999999
// MAGIC 
// MAGIC #Mensiones
// MAGIC menciones = ['@INFOPRESIDENCIA','@CANCILLERIACOL','@SENADOGOVCO','@ALCALDIACTG','@VICECOLOMBIA','@MININTERIOR','@ALCALDIAIBAGUE','@MINTRABAJOCOL','@ALCALDIACUCUTA','@MINSALUDCOL','@SFCSUPERVISOR','@BOGOTA','@DANE_COLOMBIA',
// MAGIC               '@ALCALDIADEMED','@ALCALDIADECALI','@FISCALIACOL','@ALCALDIABQUILLA']
// MAGIC bMenciones = ""
// MAGIC for identificador in range(len(menciones)):
// MAGIC   if identificador == len(menciones)-1:
// MAGIC     bMenciones = bMenciones + menciones[identificador]
// MAGIC   else:
// MAGIC     bMenciones = bMenciones + menciones[identificador] + " OR "
// MAGIC 
// MAGIC #Palabras
// MAGIC palabras = ["COVID"]
// MAGIC bPalabras = ""
// MAGIC for palabra in palabras:
// MAGIC   bPalabras = bPalabras + " " + palabra
// MAGIC 
// MAGIC #Fecha inicial busqueda menos un dia
// MAGIC fechaInicial = "2020-04-26"
// MAGIC 
// MAGIC #Fecha final busqueda mas un dia
// MAGIC fechaFinal = "2020-04-27"
// MAGIC   
// MAGIC #Query busqueda
// MAGIC query = bPalabras + " until:"+ fechaFinal + " since:" + fechaInicial + " (" + bMenciones + ")"
// MAGIC 
// MAGIC continuar = True
// MAGIC 
// MAGIC while (continuar == True):
// MAGIC    
// MAGIC   #Se crea la consulta de Twitter
// MAGIC   menor -= 1
// MAGIC   results = api.search(q=query,include_entities=True,lang="es",count=100,max_id=menor,result_type="recent",tweet_mode='extended')
// MAGIC   menor = 9999999999999999999
// MAGIC 
// MAGIC   #Se recuperan los Tweets
// MAGIC   tweets = results["statuses"]
// MAGIC     
// MAGIC   #Verifca el largo del arreglo de Tweets
// MAGIC   if len(tweets) == 0:
// MAGIC     continuar = False
// MAGIC   else: 
// MAGIC     #Se recorren los Tweets
// MAGIC     for tweet in tweets:
// MAGIC       idTweet = tweet["id"]
// MAGIC       #Se actualiza el id de descarga si es menor
// MAGIC       if idTweet < menor:
// MAGIC         menor = idTweet
// MAGIC       print("Escribiendo archivo = "+"/dbfs/twitterdata/"+str(idTweet)+".json")
// MAGIC       with open("/dbfs/twitterdata/"+str(idTweet)+".json", 'w') as outfile:
// MAGIC         escribir = json.dumps(tweet)
// MAGIC         outfile.write(escribir)
// MAGIC         outfile.close()

// COMMAND ----------

// MAGIC %md
// MAGIC ### EL RESUTADO ES EL NÚMERO DE TWITS 1 POR FICHERO

// COMMAND ----------

// MAGIC %sh 
// MAGIC 
// MAGIC ls /dbfs/twitterdata/ | wc -l

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Cargue de Tweets alojado en ficheros JSON a un Spark DataFrame con los siguientes parametros:
// MAGIC - inferSchema: true
// MAGIC - read: json Formato de los Tweets descargados
// MAGIC - repartition: 8 para reducir la lectura cuando se realice una operacion con el DataFrame
// MAGIC - cache: Cargar en memoria el DataFrame

// COMMAND ----------

val jsonFile = "dbfs:/twitterdata/"

val twitsDF = spark.read         // The DataFrameReader
  .option("inferSchema","true")  // Automatically infer data types & column names
  .json(jsonFile)               // Creates a DataFrame from JSON after reading in the file
  .repartition(8)             // COULD WE INCLUDE SOMETHING TO SPEED UP LATER OPS? YO CREO QUE SÍ :) HAY UN FICHERO POR TWIT, ESTO ES MALO PARA SPARK
  .cache()

println(twitsDF.count())

//IMPRIMIR EL SCHEMA
twitsDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Cargue del Spark DataFrame con los Tweets a la base de datos de MongoDB
// MAGIC * format: com.mongodb.spark.sql.DefaultSource, base de datos de MongoDB
// MAGIC * mode: append, no se sobre escriben los resultados
// MAGIC * option
// MAGIC   * database: twitter, base de datos en donde se almacenaran los resultados
// MAGIC   * collection: covid, coleccion donde se alojaran los ficheros JSON de los Tweets
// MAGIC * ![warning Logo Tiny](http://icons.iconarchive.com/icons/martz90/hex/32/warning-icon.png) Instalar la libreria ***org.mongodb.spark:mongo-spark-connector_2.11:2.3.1*** en el cluster

// COMMAND ----------

import org.apache.spark.sql.functions._
import com.mongodb.spark._

twitsDF.write
  .format("com.mongodb.spark.sql.DefaultSource")//Output data source
  .mode("append")//No remplaza si no que une
  .option("database", "twitter")//Base de datos
  .option("collection", "covid")//Collection
  .save()//Guarda el resultado

twitsDF.unpersist()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Data Load
// MAGIC <b>Paso 0.</b> Se importan los tweets desde MongoDB a un Spark DataFrame
// MAGIC * format: com.mongodb.spark.sql.DefaultSource, base de datos de MongoDB
// MAGIC * option
// MAGIC   * database: twitter, base de datos donde se encuentran almacenados los resultados
// MAGIC   * collection: covid, coleccion donde se encuentran los ficheros JSON con los Tweets
// MAGIC * repartition: 8, para aprovechar el procesamiento en paralelo
// MAGIC * cache: para cargar en memoria del DataFrame

// COMMAND ----------

// Utility method to count & print the number of records in each partition
def printRecordsPerPartition(df:org.apache.spark.sql.Dataset[Row]): Unit = {
  println("Per-Partition Counts:")
  
  val results = df.rdd                                   // Convert to an RDD
    .mapPartitions(it => Array(it.size).iterator, true)  // For each partition, count
    .collect()                                           // Return the counts to the driver

  results.foreach(x => println("* " + x))
}

// COMMAND ----------

import spark.implicits._

val dfCovid = spark
  .read
  .format("com.mongodb.spark.sql.DefaultSource")
  .option("database", "twitter")
  .option("collection", "covid")
  .load()
  .dropDuplicates("id_str")
  .repartition(8)
  .cache()
  
dfCovid.count()
  
println("Cantidad de particiones = "+dfCovid.rdd.partitions.size)

printRecordsPerPartition(dfCovid)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ETL Process
// MAGIC <b>Paso 1.</b> Seleccion de las columnas con la información mas relevante:
// MAGIC 
// MAGIC |Columna dfCovid|Información|Columna dfCovidSelect|
// MAGIC |:-:|:-:|:-:|
// MAGIC |created_at|Fecha de publicación del Tweet|fecha_publicacion|
// MAGIC |id_str|Id unico del Tweet|tweet_id|
// MAGIC |user.screen_name|Usuario de Twitter|twitter_user|
// MAGIC |user.name|Nombre de usuario de Twitter|twitter_user_name|
// MAGIC |user.followers_count|Cantidad de usuarios que siguen a este usuario|twitter_user_follwers|
// MAGIC |user.friends_count|Cantidad de usuarios que sigue este usuario|twitter_user_following|
// MAGIC |full_text|Texto del Tweet publicado|tweet_text|
// MAGIC |entities.hashtags.text|Array de hashtags utilizados|tweet_hashtags|
// MAGIC |entities.user_mentions.name|Array de usuarios de Twitter mencionados|tweet_mentions_user|
// MAGIC |entities.user_mentions.screen_name|Array de nombre de usuarios de Twitter mencionados en el Tweet|tweet_mentions_user_name|
// MAGIC |retweeted_status.created_at|Fecha de publicacion del Retweet|retweet_fecha_publicacion|
// MAGIC |retweeted_status.id_str|Id unico del Retweet|retweet_id|
// MAGIC |retweeted_status.user.screen_name|Si es Retweet, usuario de Twitter|retweet_user|
// MAGIC |retweeted_status.user.name|Si es Retweet, el nombre del usuario de Twitter|retweet_user_name|
// MAGIC |retweeted_status.full_text|Si es Retweet, el texto publicado|retweet_text|
// MAGIC |retweeted_status.entities.hashtags.text|Si es Retweet, Array de hashtags utilizados|retweet_text|
// MAGIC |retweeted_status.entities.user_mentions.screen_name|Si es Retweet, Array de usuarios de Twitter mencionados|retweet_mentions_user|
// MAGIC |retweeted_status.entities.user_mentions.name|Si es Retweet, Array de nombre de usuarios de Twitter|retweet_mentions_user_name|

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfCovidSelect = dfCovid
  .withColumn("mes",substring($"created_at",5,3))
  .withColumn("dia",substring($"created_at",9,2))
  .withColumn("hh:mm:ss",substring($"created_at",12,8))
  .withColumn("year",substring($"created_at",27,4))
  .withColumn("fecha_publicacion",concat($"year",lit("-"),$"mes",lit("-"),$"dia",lit(" "),$"hh:mm:ss"))
  .withColumn("fecha_publicacion",unix_timestamp($"fecha_publicacion","yyyy-MMM-dd HH:mm:ss").cast("Timestamp"))
  .withColumn("retweet_mes",substring($"retweeted_status.created_at",5,3))
  .withColumn("retweet_dia",substring($"retweeted_status.created_at",9,2))
  .withColumn("retweet_hh:mm:ss",substring($"retweeted_status.created_at",12,8))
  .withColumn("retweet_year",substring($"retweeted_status.created_at",27,4))
  .withColumn("retweet_fecha_publicacion",concat($"retweet_year",lit("-"),$"retweet_mes",lit("-"),$"retweet_dia",lit(" "),$"retweet_hh:mm:ss"))
  .withColumn("retweet_fecha_publicacion",unix_timestamp($"retweet_fecha_publicacion","yyyy-MMM-dd HH:mm:ss").cast("Timestamp"))
  .select($"fecha_publicacion",
          $"id_str".alias("tweet_id"),
          $"user.screen_name".alias("twitter_user"),
          $"user.name".alias("twitter_user_name"),
          $"user.followers_count".alias("twitter_user_followers"),
          $"user.friends_count".alias("twitter_user_following"),
          $"full_text".alias("tweet_text"),
          $"entities.hashtags.text".alias("tweet_hashtags"),
          $"entities.user_mentions.screen_name".alias("tweet_mentions_user"),
          $"entities.user_mentions.name".alias("tweet_mentions_user_name"),
          $"retweet_fecha_publicacion",
          $"retweeted_status.id_str".alias("retweet_id"),
          $"retweeted_status.user.screen_name".alias("retweet_user"),
          $"retweeted_status.user.name".alias("retweet_user_name"),
          $"retweeted_status.full_text".alias("retweet_text"),
          $"retweeted_status.entities.hashtags.text".alias("retweet_hashtags"),
          $"retweeted_status.entities.user_mentions.screen_name".alias("retweet_mentions_user"),
          $"retweeted_status.entities.user_mentions.name".alias("retweet_mentions_user_name"))
  .cache()

dfCovidSelect.count()

dfCovid.unpersist()

display(dfCovidSelect.limit(5))

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 2.</b> Realizar algunas modifiaciones cuando es un Retweet, esto se debe a que la informacion del Tweet no esta totalmente completa debido a las limitaciones de Twitter
// MAGIC * tweet_text, en caso de que sea un Retwwet se remplaza el texto con formato de Retweet ("RT @...")
// MAGIC * tweet_hashtags, se combinan los hashtags unicos que estan actualmente en el Tweet y el Retweet
// MAGIC * tweet_mentions_user, se combinan los usuarios unicos mencionados que estan actualmente en el Tweet y el Retweet

// COMMAND ----------

val dfCovidSelectUpdate =  dfCovidSelect
  .withColumn("tweet_text",when($"retweet_id".isNull,$"tweet_text").otherwise(concat(lit("RT @"),$"tweet_mentions_user".getItem(0),lit(": "),$"retweet_text")))
  .withColumn("tweet_hashtags",array_union($"tweet_hashtags",$"retweet_hashtags"))
  .withColumn("tweet_mentions_user",array_union($"tweet_mentions_user",$"retweet_mentions_user"))
  .withColumn("tweet_mentions_user_name",array_union($"tweet_mentions_user_name",$"retweet_mentions_user_name"))
  .cache()

dfCovidSelectUpdate.count()

dfCovidSelect.unpersist()

display(dfCovidSelectUpdate.limit(5))

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 3.</b> Convertir la columna de fecha_publicacion en formato GMT+0 a GMT-5 (Hora local en Colombia)
// MAGIC * Para realizar operaciones sobre esta fecha se agregaran las siguientes columnas:
// MAGIC   * year, año en el que se realiza la publicacion
// MAGIC   * month, mes del año en el que se realiza la publicacion
// MAGIC   * day, dia del mes en el que se realiza la publicacion
// MAGIC   * day_of_week, dia de la semana en el que se realiza la publicacion
// MAGIC   * hour, hora del dia en el que se realiza la publicacion

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val dfCovidFinal = dfCovidSelectUpdate
  .withColumn("fecha_local",date_format(from_utc_timestamp($"fecha_publicacion","GMT-5"),"yyy-MM-dd HH:mm:ss"))
  .withColumn("year",year($"fecha_local"))
  .withColumn("month",month($"fecha_local"))
  .withColumn("day",dayofmonth($"fecha_local"))
  .withColumn("day_of_week",date_format($"fecha_local","EEEE"))
  .withColumn("hour",hour($"fecha_local"))
  .withColumn("retweet_fecha_local",date_format(from_utc_timestamp($"fecha_publicacion","GMT-5"),"yyy-MM-dd HH:mm:ss"))
  .select($"fecha_local",
         $"year",
         $"month",
         $"day",
         $"day_of_week",
         $"hour",
         $"tweet_id",
         $"twitter_user",
         $"twitter_user_name",
         $"twitter_user_followers",
         $"twitter_user_following",
         $"tweet_text",
         $"tweet_hashtags",
         $"tweet_mentions_user",
         $"tweet_mentions_user_name",
         $"retweet_fecha_local",
         $"retweet_id",
         $"retweet_user",
         $"retweet_user_name",
         $"retweet_text",
         $"retweet_hashtags",
         $"retweet_mentions_user",
         $"retweet_mentions_user_name")
  .cache()

dfCovidFinal.count()

dfCovidSelectUpdate.unpersist()

display(dfCovidFinal.limit(5))

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 4.</b> Consulta del intervalo de tiempo de los Tweets

// COMMAND ----------

import org.apache.spark.sql.functions._

val fechas = dfCovidFinal
  .select(min($"fecha_local"),max($"fecha_local"))
  .first()

println("Fecha minima de descarga = "+fechas.get(0))
println("Fecha maxima de descarga = "+fechas.get(1))

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 5.</b> Se comparan los intervalos de tiempo para determinar si se tienen datos asimetricos

// COMMAND ----------

import org.apache.spark.sql.functions._

display(dfCovidFinal
       .select($"day_of_week")
       .orderBy($"fecha_local")
       .groupBy($"day_of_week")
       .count())

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 6.</b> Exportacion del DataFrame despues del proceso de ETL a MongoDB
// MAGIC * format: com.mongodb.spark.sql.DefaultSource, base de datos de MongoDB
// MAGIC * mode: overwrite, se remplazan los archivos ya existentes
// MAGIC * option
// MAGIC   * database: twitter, base de datos donde se encuentran almacenados los resultados
// MAGIC   * collection: covid_dwh, coleccion donde se encuentran los ficheros JSON con los Tweets
// MAGIC * repartition: 8, para aprovechar el procesamiento en paralelo

// COMMAND ----------

import org.apache.spark.sql.functions._
import com.mongodb.spark._

dfCovidFinal.write
  .format("com.mongodb.spark.sql.DefaultSource")//Output data source
  .mode("overwrite")//Se remplaza el set de datos
  .option("database", "twitter")//Base de datos
  .option("collection", "covid_dwh")//Collection
  .save()//Guarda el resultado

dfCovidFinal.unpersist()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Data analysis
// MAGIC Para iniciar el analisis se recupera la informacion almacenada en la BBDD de MongoDB. La informacion cargada se utilizara para las consultas que no necesiten ningun tratamiento adicional de datos  
// MAGIC <b>Paso 7.</b> Cargue de datos desde MongoDB
// MAGIC * format: com.mongodb.spark.sql.DefaultSource, base de datos de MongoDB
// MAGIC * option
// MAGIC   * database: twitter, base de datos donde se encuentran almacenados los resultados
// MAGIC   * collection: covid_dwh, coleccion donde se encuentran los ficheros JSON estrandarizados con los Tweets
// MAGIC   * repartition: 8, para aprovechar el procesamiento en paralelo
// MAGIC   * cache: para cargar en memoria del DataFrame

// COMMAND ----------

import com.mongodb.spark._

val dfAnalysis = spark
  .read
  .format("com.mongodb.spark.sql.DefaultSource")
  .option("database", "twitter")
  .option("collection", "covid_dwh")
  .load()
  .drop("_id")
  .withColumn("fecha_local",$"fecha_local".cast("Timestamp"))
  .withColumn("retweet_fecha_local",$"retweet_fecha_local".cast("Timestamp"))
  .repartition(8)
  .cache()
  
println("Cantidad de registros cargados = "+dfAnalysis.count())
  
println("Cantidad de particiones = "+dfAnalysis.rdd.partitions.size)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 8.</b> Cantidad de Tweets que se realizan por hora
// MAGIC - Horas pico entre las 4 y 6 pm

// COMMAND ----------

import org.apache.spark.sql.functions._

display(dfAnalysis
       .select($"hour")
       .orderBy($"hour")
       .groupBy($"hour")
       .count())

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 9.</b> Cantidad de Tweets por dia en las horas pico
// MAGIC - En promedio todos los dias se comportarn igual durante esta franja horaria

// COMMAND ----------

import org.apache.spark.sql.functions._

display(dfAnalysis
       .filter($"hour" >= 16 and $"hour" <= 18)
       .select($"hour",concat($"day",lit("-"),$"day_of_week").alias("day_of_week"))
       .groupBy($"hour",$"day_of_week")
       .count()
       .orderBy($"day_of_week",$"hour"))

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 10.</b> Principales usuarios mencionados en las horas pico de Tweets
// MAGIC - El principal usuario mencionado es el Ministerio de Salud de Colombia, que es el principal ente regulador de la situacion del covid

// COMMAND ----------

import org.apache.spark.sql.functions._

display(dfAnalysis
       .filter($"hour" >= 16 and $"hour" <= 18)
       .select(explode($"tweet_mentions_user_name").alias("tweet_mentions_user_name"))
       .groupBy($"tweet_mentions_user_name")  
       .count()
       .orderBy($"count".desc)
       .limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 11.</b> Principales hastags utilizados en las horas pico
// MAGIC - El principal Hashtag que se utiliza el es ReporteCOVID19 que contiene la información del comportamiento diario de los casos en el pais

// COMMAND ----------

display(dfAnalysis
        .filter($"hour" >= 16 and $"hour" <= 18)
        .select(explode($"tweet_mentions_user_name").alias("tweet_mentions_user_name"),$"tweet_hashtags")
        .filter($"tweet_mentions_user_name" === "MinSaludCol")
        .select(explode($"tweet_hashtags").alias("tweet_hashtags"))
        .groupBy($"tweet_hashtags")
        .count()
        .orderBy($"count".desc)
        .limit(10)) 

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 12.</b> Principales hashtags que no contienen la palabra Covid
// MAGIC - Noticias de cuidado personal y como afrontar la cuarentena

// COMMAND ----------

display(dfAnalysis
        .filter($"hour" >= 16 and $"hour" <= 18)
        .select(explode($"tweet_mentions_user_name").alias("tweet_mentions_user_name"),$"tweet_hashtags")
        .filter($"tweet_mentions_user_name" === "MinSaludCol")
        .select(explode($"tweet_hashtags").alias("tweet_hashtags"))
        .filter(!lower($"tweet_hashtags").contains("covid"))
        .groupBy($"tweet_hashtags")
        .count()
        .orderBy($"count".desc)
        .limit(10)) 

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 13.</b> Funcion que retorna un DataFrame con los usuarios que han realizado un Retweet especifico
// MAGIC - retweet_id = Id del tweet original que se publico
// MAGIC - dfInicial = DataFrame inicial que consolida todos los Tweets

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

def tweetCoverage(retweet_id: String, dfInicial: DataFrame): DataFrame = {
  
  val dfRetweet = dfInicial
  .filter($"retweet_id" === retweet_id)
  .groupBy($"retweet_id")
  .agg(min($"retweet_fecha_local").alias("min_fecha"))
  
  val dfJoinRetweet = dfInicial
  .join(dfRetweet,dfInicial("retweet_id") === dfRetweet("retweet_id"))
  .withColumn("time_diference",(dfInicial("retweet_fecha_local").cast("Long") - dfRetweet("min_fecha").cast("Long"))/3600D)
  .selectExpr("twitter_user_followers","floor(time_diference) as time_diference_hours")
  .groupBy($"time_diference_hours")
  .agg(sum("twitter_user_followers").alias("users_coverage"))
  .orderBy($"time_diference_hours".asc)
  
  return dfJoinRetweet
}

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 14.</b> Potencial de la cantidad de usuarios que han visto un Tweet especifico

// COMMAND ----------

val retweet_id_look = dfAnalysis
  .withColumn("tweet_hashtags",explode($"tweet_hashtags"))
  .filter(lower($"tweet_hashtags").contains("reportecovid19"))
  .filter($"day" =!= 26)
  .groupBy($"retweet_id")
  .agg(max("retweet_fecha_local").alias("max_fecha"))
  .orderBy($"max_fecha".desc)
  .first()
  .getString(0)

val dfTweetCoverage = tweetCoverage(retweet_id_look,dfAnalysis)

display(dfTweetCoverage)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Grafico de nube de palabras
// MAGIC <b>Paso 15.</b> Creacion de funciones necesarias
// MAGIC - removeAccents, se encarga de eliminar las tildes que tiene una palabra
// MAGIC - isDouble, validad si una palabra es un numero

// COMMAND ----------

import org.apache.commons.lang3.StringUtils.{stripAccents,isNumeric}

def removeAccents(text: String): String = 
{
  return stripAccents(text)
}

val removeAccent = spark.udf.register("removeAccent", (text: String) => removeAccents(text))

def isDouble(text: String): Boolean =
{
  return isNumeric(text)
}

val isNumber = spark.udf.register("isNumber", (text: String) => isDouble(text))

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 16.</b> Se crea un DataFrame con la información de cada una de las palabras utilizadas en los Tweets, se aplican las siguientes condiciones:
// MAGIC - Eliminar URLs, palabra RT, menciones realizadas a los diferentes ususarios, emojis y caracteres especiales
// MAGIC - Emplazar el caracter # que se utiliza en los hashtags
// MAGIC - Tomar unicamente palabras que tengan largo mayor a 1 y no sean numeros

// COMMAND ----------

import org.apache.spark.sql.functions._

val wordCloud = dfAnalysis
  .withColumn("tweet_text",removeAccent(lower($"tweet_text")))//Remueve los acentos de las palabras del tweet
  .withColumn("words",explode(split($"tweet_text"," ")))//Se expanden las palabras del tweet
  .select($"words")//Selecciona unicamente la columna de palabras
  .filter(!$"words".contains("http") && $"words" =!= "rt" && !$"words".contains("@"))//Elimina las palabras que son urls,rt o es una mencion
  .withColumn("words",regexp_replace($"words", "#",""))//Se remplaza el # del hashtag
  .withColumn("words",explode(split($"words","[^a-zA-Z0-9\\s+]")))//Se eliminan emojis y caracteres especiales
  .withColumn("words",explode(split($"words","\n")))//Se eliminan los saltos de linea
  .withColumn("isNumeric",isNumber($"words"))//Se valida si la palabra es numerica
  .filter($"words" =!= "" && $"isNumeric" === "false" && length($"words") > 1)//Se filtra las palabras que tienen contenido,no son numeros y tienen largo mayor a 1
  .select($"words")//Se selecciona la columna de palabras
  .groupBy($"words")
  .count()

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 17.</b> Escritura del DataFrame de palabra en MongoDB
// MAGIC - format = "com.mongodb.spark.sql.DefaultSource", Formato de conexion a MongoDB
// MAGIC - mode = "overwrite", Se remplaza si ya existe esta coleccion
// MAGIC - option
// MAGIC   - database = "twitter", Base de datos donde se esta alojando la informacion
// MAGIC   - collection = "words", Coleccion donde se guardan las palabras

// COMMAND ----------

import com.mongodb.spark._

wordCloud.write
  .format("com.mongodb.spark.sql.DefaultSource")//Output data source
  .mode("overwrite")//Se remplaza el set de datos
  .option("database", "twitter")//Base de datos
  .option("collection", "words")//Collection
  .save()//Guarda el resultado

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 18.</b> Intalacion de algunas librerias en el ambiente de Python
// MAGIC - spacy, Procesamiento de textos
// MAGIC - nltk, Procesamiento de textos
// MAGIC - matplotlib, Manejo de graficos
// MAGIC - wordcloud, Grafico de nube de palabras

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC !pip install -U spacy
// MAGIC !pip install nltk
// MAGIC !pip install matplotlib
// MAGIC !pip install wordcloud

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 19.</b> Recuperacion de las palabras desde MongoDB, para trabajar en el ambiente de Python se convertira el Spark DataFrame en un Pandas DataFrame
// MAGIC - format = "com.mongodb.spark.sql.DefaultSource", Formato de conexcion a MOngoDB
// MAGIC - option
// MAGIC   - database = "twitter", Base de datos donde se aloja la informacion
// MAGIC   - collection = "words", Coleccion donde se almacenan las palabras

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC dfWords = (spark
// MAGIC       .read
// MAGIC       .format("com.mongodb.spark.sql.DefaultSource")
// MAGIC       .option("database", "twitter")
// MAGIC       .option("collection", "words")
// MAGIC       .load()
// MAGIC       .drop("_id")
// MAGIC       .repartition(8)
// MAGIC       .cache()
// MAGIC       )
// MAGIC 
// MAGIC print("Cantidad de registros cargados = "+str(dfWords.count()))
// MAGIC 
// MAGIC words = dfWords.toPandas()
// MAGIC 
// MAGIC words.set_index('words',inplace=True)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 20.</b> Eliminacion de palabras que no aportan ningun significado (STOPWORDS), utilizando las librerias de manejo de texto instaladas previamente

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC from spacy.lang.es.stop_words import STOP_WORDS
// MAGIC 
// MAGIC import nltk
// MAGIC 
// MAGIC nltk.download('stopwords')
// MAGIC 
// MAGIC from nltk.corpus import stopwords
// MAGIC 
// MAGIC stop_words_sp = set(stopwords.words('spanish'))
// MAGIC 
// MAGIC eliminar = ['covid','casos']
// MAGIC 
// MAGIC print(words.count())
// MAGIC 
// MAGIC for palabra in words.index:
// MAGIC 
// MAGIC   if palabra in STOP_WORDS:
// MAGIC     eliminar.append(palabra)
// MAGIC   elif palabra in stop_words_sp:
// MAGIC     eliminar.append(palabra)
// MAGIC     
// MAGIC words.drop(eliminar,axis=0,inplace=True)
// MAGIC 
// MAGIC print(words.count())

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 21.</b> Creacion del grafico de nube de palabras
// MAGIC - Las principales palabras que se encuentran en los tweets hace referencia principalmente a 3 ambitos:
// MAGIC   - Evolución de la pandemia, casos diagnosticados, recuperados y fallecidos
// MAGIC   - Medidas de prevencion para reducir el avance de la pandemia
// MAGIC   - Acciones economicas y sociales que estan tomando el gobierno nacional y los gobiernos distritales

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC from wordcloud import WordCloud
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC textWords = words.to_dict("dict")["count"]
// MAGIC 
// MAGIC wordcloud = WordCloud(width = 1500, height = 1500, background_color ='white',min_font_size = 10,max_words=100,prefer_horizontal=1)
// MAGIC 
// MAGIC wordcloud.generate_from_frequencies(textWords) 
// MAGIC 
// MAGIC fig, ax = plt.subplots(figsize=(15,15))
// MAGIC plt.imshow(wordcloud)
// MAGIC plt.axis("off")
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Sentiment analysis
// MAGIC <b>Paso 22.</b> Instalacion de algunas librerias en el ambiente de Python
// MAGIC - spacy, Manejo de textos
// MAGIC - nltk, Manejo de textos
// MAGIC - textblod, Medidas de sentimientos sobre textos
// MAGIC - googletrans, Traductor de google
// MAGIC - translate, Traductor de inMemory

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC !pip install -U spacy
// MAGIC !pip install nltk
// MAGIC !pip install -U textblob
// MAGIC !python -m textblob.download_corpora
// MAGIC !pip install googletrans
// MAGIC !pip install translate

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 23.</b> Recuperacion de los Tweets desde MongoDB
// MAGIC - format = "com.mongodb.spark.sql.DefaultSource", Formato de conexcion MongoDB
// MAGIC - option
// MAGIC   - database = "twitter", Base de datos donde se esta almacenando la informacion
// MAGIC   - collection = "covid_dwh", Coleccion con la información procesada de los Tweets

// COMMAND ----------

import com.mongodb.spark._

val dfAnalysis = spark
  .read
  .format("com.mongodb.spark.sql.DefaultSource")
  .option("database", "twitter")
  .option("collection", "covid_dwh")
  .load()
  .drop("_id")
  .withColumn("fecha_local",$"fecha_local".cast("Timestamp"))
  .withColumn("retweet_fecha_local",$"retweet_fecha_local".cast("Timestamp"))
  .repartition(8)
  .cache()

println("Cantidad de registros cargados = "+dfAnalysis.count()) 
println("Cantidad de particiones = "+dfAnalysis.rdd.partitions.size)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 24.</b> Creacion de funciones necesarias para el tratamiento de datos
// MAGIC - removeAccents, Funcion encargada de suprimir las tildes en las palabras

// COMMAND ----------

import org.apache.commons.lang3.StringUtils.stripAccents

def removeAccents(text: String): String = 
{
  return stripAccents(text)
}

val removeAccent = spark.udf.register("removeAccent", (text: String) => removeAccents(text))

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 25.</b> Se crea el DataFrame que contiene el texto del tweet, teniendo en cuenta estos criterios
// MAGIC - Remplazar los # que son utilizados en los hashtags
// MAGIC - El texto debe estar contenido en un array, separando cada una de las palabras

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfSentimentAnalysis = dfAnalysis
  .withColumn("tweet_text",removeAccent(lower($"tweet_text")))//Todo el texto en minuscula
  .withColumn("tweet_text",regexp_replace($"tweet_text", "#",""))//Se remplazan los hashtags
  .withColumn("tweet_text",regexp_replace($"tweet_text","\n"," "))//Se remplazan los saltos de linea
  .withColumn("tweet_text_split",split($"tweet_text"," "))
  .select($"tweet_id",$"twitter_user",$"twitter_user_name",$"tweet_text",$"tweet_text_split")

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 26.</b> Se escribe el DataFrame del comando anterior en MongoDB
// MAGIC - format = "com.mongodb.spark.sql.DefaultSource", Formato de conexion a MongoDB
// MAGIC - mode = "overwerite", Se remplaza la coleccion si existe
// MAGIC - option
// MAGIC   - database = "twitter", Base de datos donde se esta almacenando la informacion
// MAGIC   - collection = "sentiment_analysis", Coleccion que guaradara la informacion inicial para realizar el Sentiment Analysis

// COMMAND ----------

dfSentimentAnalysis.write
  .format("com.mongodb.spark.sql.DefaultSource")//Output data source
  .mode("overwrite")//Se remplaza el set de datos
  .option("database", "twitter")//Base de datos
  .option("collection", "sentiment_analysis")//Collection
  .save()//Guarda el resultado

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 27.</b> Se recupera la información desde MongoDB, para ser procesada en Python
// MAGIC - format "com.mongodb.spark.sql.DefaultSource", Formato de conexion a MongoDB
// MAGIC - option
// MAGIC   - database = "twitter", Base de datos donde se esta almacenando la informacion
// MAGIC   - collection = "sentiment_analysis", Coleccion con la informacion procesa en el paso anterior

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC dfSentimentAnalysis = (spark
// MAGIC       .read
// MAGIC       .format("com.mongodb.spark.sql.DefaultSource")
// MAGIC       .option("database", "twitter")
// MAGIC       .option("collection", "sentiment_analysis")
// MAGIC       .load()
// MAGIC       .drop("_id")
// MAGIC       .repartition(8)
// MAGIC       .cache()
// MAGIC       )
// MAGIC 
// MAGIC print("Cantidad de registros cargados = "+str(dfSentimentAnalysis.count()))
// MAGIC 
// MAGIC pandasDFSentimentAnalysis = dfSentimentAnalysis.toPandas()

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 28.</b> Se realiza el procesamiento del texto eliminando palabras que no agregan significado al tweet, teniendo en cuenta las siguientes reglas:
// MAGIC - Eliminacion de URLs, palabra RT, menciones a usuarios
// MAGIC - Remplazo de caracteres especiales y emojis
// MAGIC - Elminacion de STOPWORDS contenidas en las librerias de trabajo de texto  
// MAGIC Por ultimo se crea la columna con el texto del tweet ya modificado

// COMMAND ----------

// MAGIC %python 
// MAGIC import pandas as pd
// MAGIC import re
// MAGIC 
// MAGIC from spacy.lang.es.stop_words import STOP_WORDS
// MAGIC 
// MAGIC import nltk
// MAGIC 
// MAGIC nltk.download('stopwords')
// MAGIC 
// MAGIC from nltk.corpus import stopwords
// MAGIC 
// MAGIC stop_words_sp = set(stopwords.words('spanish'))
// MAGIC 
// MAGIC tweetsFilter = []
// MAGIC 
// MAGIC for counter in pandasDFSentimentAnalysis.index:
// MAGIC   arrayWords = pandasDFSentimentAnalysis.loc[counter,"tweet_text_split"]
// MAGIC   tweet_text_filter = ""  
// MAGIC   
// MAGIC   for word in arrayWords:
// MAGIC     if word.find("rt") == -1 and word.find("@") == -1 and word.find("http") == -1:
// MAGIC       if re.sub('[^a-zA-Z0-9\s-]+', '',word) != "":
// MAGIC         if word not in STOP_WORDS and word not in stop_words_sp:
// MAGIC           tweet_text_filter = tweet_text_filter + " " +  re.sub('[^a-zA-Z0-9\s-]+', '',word)
// MAGIC   
// MAGIC   tweetsFilter.append(tweet_text_filter.lstrip())
// MAGIC   
// MAGIC tweetsFilter = pd.DataFrame(tweetsFilter,columns=["tweet_filter"])
// MAGIC 
// MAGIC pandasDFSentimentAnalysisFull = pd.concat([pandasDFSentimentAnalysis,tweetsFilter],axis=1)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 29.</b> Dado que los Tweets se encuentran en Español, sera necesario traducirlos a Ingles, ya que la libreria de analisis de sentimiento procesa textos en este ultimo idioma
// MAGIC - Dado que el API de GoogleTranslate solo deja cierta cantidad de request se implemento una segunda opcion

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC from googletrans import Translator as googleTrans
// MAGIC from translate import Translator as trans
// MAGIC from math import ceil
// MAGIC 
// MAGIC traductor = googleTrans()
// MAGIC traductor2 = trans(from_lang="es",to_lang="en")
// MAGIC traduccion = []
// MAGIC 
// MAGIC for i in range(len(pandasDFSentimentAnalysisFull['tweet_filter'])):
// MAGIC   textoTraducir = pandasDFSentimentAnalysisFull.loc[i,"tweet_filter"]
// MAGIC   try:
// MAGIC     traducir = traductor.translate(textoTraducir,src='es',dest='en').text
// MAGIC     traduccion.append(traducir)
// MAGIC   except Exception:
// MAGIC     traducir = traductor2.translate(textoTraducir)
// MAGIC     traduccion.append(traducir)
// MAGIC 
// MAGIC traduccion = pd.DataFrame(traduccion,columns=["tweet_filter_en"])
// MAGIC pandasDFSentimentAnalysisFull = pd.concat([pandasDFSentimentAnalysisFull,traduccion],axis=1)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 30.</b> Se escribe el DataFrame del comando anterior en MongoDB
// MAGIC - format = "com.mongodb.spark.sql.DefaultSource", Formato de conexion a MongoDB
// MAGIC - mode = "overwerite", Se remplaza la coleccion si existe
// MAGIC - option
// MAGIC   - database = "twitter", Base de datos donde se esta almacenando la informacion
// MAGIC   - collection = "sentiment_analysis_translate", Coleccion que guaradara la informacion inicial para realizar el Sentiment Analysis Traducido

// COMMAND ----------

// MAGIC %python
// MAGIC dfSentimentAnalysisFull = spark.createDataFrame(pandasDFSentimentAnalysisFull)
// MAGIC 
// MAGIC (dfSentimentAnalysisFull.write
// MAGIC   .format("com.mongodb.spark.sql.DefaultSource")#Output data source
// MAGIC   .mode("overwrite")#Se remplaza el set de datos
// MAGIC   .option("database", "twitter")#Base de datos
// MAGIC   .option("collection", "sentiment_analysis_translate")#Collection
// MAGIC   .save())#Guarda el resultado

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 31.</b> Se realiza el analisis de sentimiento de los textos de los Tweets, utilizando la libreria TextBlob

// COMMAND ----------

// MAGIC %python
// MAGIC from textblob import TextBlob
// MAGIC 
// MAGIC dfSentimentAnalysisTranslate = (spark
// MAGIC       .read
// MAGIC       .format("com.mongodb.spark.sql.DefaultSource")
// MAGIC       .option("database", "twitter")
// MAGIC       .option("collection", "sentiment_analysis_translate")
// MAGIC       .load()
// MAGIC       .drop("_id")
// MAGIC       .repartition(8)
// MAGIC       .cache()
// MAGIC       )
// MAGIC 
// MAGIC print("Cantidad de registros cargados = "+str(dfSentimentAnalysisTranslate.count()))
// MAGIC 
// MAGIC pandasDFSentimentAnalysisTranslate = dfSentimentAnalysisTranslate.toPandas()
// MAGIC 
// MAGIC pol = lambda x: TextBlob(x).sentiment.polarity
// MAGIC sub = lambda x: TextBlob(x).sentiment.subjectivity
// MAGIC 
// MAGIC pandasDFSentimentAnalysisTranslate['polaridad'] = pandasDFSentimentAnalysisTranslate['tweet_filter_en'].apply(pol)
// MAGIC pandasDFSentimentAnalysisTranslate['subjetividad'] = pandasDFSentimentAnalysisTranslate['tweet_filter_en'].apply(sub)
// MAGIC 
// MAGIC pandasDFSentimentAnalysisTranslate.head(5)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 32.</b> Se escribe el DataFrame del comando anterior en MongoDB
// MAGIC - format = "com.mongodb.spark.sql.DefaultSource", Formato de conexion a MongoDB
// MAGIC - mode = "overwerite", Se remplaza la coleccion si existe
// MAGIC - option
// MAGIC   - database = "twitter", Base de datos donde se esta almacenando la informacion
// MAGIC   - collection = "sentiment_analysis_results", Coleccion que guaradara la informacion resultante del Sentiment Analysis

// COMMAND ----------

// MAGIC %python
// MAGIC dfSentimentAnalysisResults = spark.createDataFrame(pandasDFSentimentAnalysisTranslate)
// MAGIC 
// MAGIC (dfSentimentAnalysisResults.write
// MAGIC   .format("com.mongodb.spark.sql.DefaultSource")#Output data source
// MAGIC   .mode("overwrite")#Se remplaza el set de datos
// MAGIC   .option("database", "twitter")#Base de datos
// MAGIC   .option("collection", "sentiment_analysis_results")#Collection
// MAGIC   .save())#Guarda el resultado

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 33.</b> Se recupera la informacion resultante del analisis de sentimientos y la descargada de Twitter

// COMMAND ----------

import com.mongodb.spark._

val dfSentimentAnalysis = spark
  .read
  .format("com.mongodb.spark.sql.DefaultSource")
  .option("database", "twitter")
  .option("collection", "sentiment_analysis_results")
  .load()
  .drop("_id")
  .select($"tweet_id",$"polaridad",$"subjetividad",$"tweet_filter")
  .repartition(8)
  .cache()
  
println("Cantidad de registros cargados = "+dfSentimentAnalysis.count())
println("Cantidad de particiones = "+dfSentimentAnalysis.rdd.partitions.size)

val dfTweetInfo = spark
  .read
  .format("com.mongodb.spark.sql.DefaultSource")
  .option("database", "twitter")
  .option("collection", "covid_dwh")
  .load()
  .drop("_id")
  .withColumn("fecha_local",$"fecha_local".cast("Timestamp"))
  .withColumn("retweet_fecha_local",$"retweet_fecha_local".cast("Timestamp"))
  .repartition(8)
  .cache()

println("Cantidad de registros cargados = "+dfTweetInfo.count())
println("Cantidad de particiones = "+dfTweetInfo.rdd.partitions.size)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 34.</b> Se asigna el resultado del analsis de sentimientos a cada uno de los Tweets

// COMMAND ----------

val dfSentimentAnalysisFull = dfTweetInfo
  .join(dfSentimentAnalysis,dfTweetInfo("tweet_id")===dfSentimentAnalysis("tweet_id"),"left")
  .drop(dfSentimentAnalysis("tweet_id"))
  .repartition(8)
  .cache()

println("Cantidad de registros cargados = "+dfSentimentAnalysisFull.count())
println("Cantidad de particiones = "+dfSentimentAnalysisFull.rdd.partitions.size)
        
dfTweetInfo.unpersist()
dfSentimentAnalysis.unpersist()

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 35.</b> Grafica de la cantidad de Tweets por sentimiento
// MAGIC - Segun la documentacion de la libreria de TextBlob, la polaridad es la encargada de identificar el sentimiento, para este caso se realiza los siguientes intevalos:
// MAGIC   - 1-negativa, para -1 <= polaridad < 0
// MAGIC   - 2-neutro, polaridad - 0
// MAGIC   - 3-positivo, 0 < polaridad <= 1
// MAGIC - En los siguientes pasos se realizara un analisis mas espesifico sobre los Tweets con sentimiento negativo y positvo

// COMMAND ----------

import org.apache.spark.sql.functions._

display(dfSentimentAnalysisFull
         .withColumn("polaridad_texto",when($"polaridad" < 0,"1-negativa")
                                       .otherwise(when($"polaridad" === 0,"2-neutro").otherwise("3-positivo")))
         .groupBy($"polaridad_texto")
         .count()
         .orderBy($"polaridad_texto".asc))

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 36.</b> Instalacion de las siguientes librerias en el ambiente de Python
// MAGIC - matplotlib, Manejo de graficos
// MAGIC - wordcloud, Grafico de nube de palabras

// COMMAND ----------

// MAGIC %python
// MAGIC !pip install matplotlib #Manejo de graficos
// MAGIC !pip install wordcloud #Grafico de nube de palabras

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 37.</b> Creacion de funciones necesarias para el analisis
// MAGIC - filterDFDayPolaridadNegativa, Funcion encargada de recuperar la informacion de los usuarios mas mencionados por dia y cuya polaridad es negativa
// MAGIC - filterDFDayPolaridadNegativa, Funcion encargada de recuperar la informacion de los usuarios mas mencionados por dia y cuya polaridad es positiva
// MAGIC - En las 2 funciones anteriores no se tendra en cuenta la cuenta del Ministerio de Salud de Colombia, ya que como se evidencio previamente es el usuario mas mencionado y gran parte de sus menciones hacen referencia al reporte del covid que se hace diariamente
// MAGIC - isDouble, Identifica si una palabra es numero o no

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.commons.lang3.StringUtils.isNumeric

def filterDFDayPolaridadNegativa(dfFiltar: DataFrame,dia: Integer): DataFrame = {
  dfFiltar
    .filter($"polaridad" < 0 && $"day" === dia)
    .select(concat($"day",lit("-"),$"day_of_week").alias("day_of_week"),explode($"tweet_mentions_user").alias("tweet_mentions_user"))
    .filter($"tweet_mentions_user" =!= "MinSaludCol")//Es el que mas tweets tiene y son casi todos del reporte
    .groupBy($"day_of_week",$"tweet_mentions_user")
    .count()
    .orderBy($"count".desc)
    .limit(5)
}

def filterDFDayPolaridadPositiva(dfFiltar: DataFrame,dia: Integer): DataFrame = {
  dfFiltar
    .filter($"polaridad" > 0 && $"day" === dia)
    .select(concat($"day",lit("-"),$"day_of_week").alias("day_of_week"),explode($"tweet_mentions_user").alias("tweet_mentions_user"))
    .filter($"tweet_mentions_user" =!= "MinSaludCol")
    .groupBy($"day_of_week",$"tweet_mentions_user")
    .count()
    .orderBy($"count".desc)
    .limit(5)
}

def isDouble(text: String): Boolean =
{
  return isNumeric(text)
}

val isNumber = spark.udf.register("isNumber", (text: String) => isDouble(text))

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 38.</b> Grafico de los usuarios mas mencionados en el periodo de analisis cuyo Tweet tiene sentimiento negativo
// MAGIC - Cuentas de Twitter referentes al Gobierno Nacional
// MAGIC - Cuentas de Twitter referentes a la ciudad de Bogotá
// MAGIC - Omar Bula, usuario que es tendencia

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfNeg21 = filterDFDayPolaridadNegativa(dfSentimentAnalysisFull,21)
val dfNeg22 = filterDFDayPolaridadNegativa(dfSentimentAnalysisFull,22)
val dfNeg23 = filterDFDayPolaridadNegativa(dfSentimentAnalysisFull,23)
val dfNeg24 = filterDFDayPolaridadNegativa(dfSentimentAnalysisFull,24)
val dfNeg25 = filterDFDayPolaridadNegativa(dfSentimentAnalysisFull,25)
val dfNeg26 = filterDFDayPolaridadNegativa(dfSentimentAnalysisFull,26)

val dfNeg = dfNeg21
  .unionAll(dfNeg22)
  .unionAll(dfNeg23)
  .unionAll(dfNeg24)
  .unionAll(dfNeg25)
  .unionAll(dfNeg26)

display(dfNeg)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 39.</b> Se grafica las palabras mas utilizadas dentro del Tweets con sentimiento negativo, relacionadas con el Gobierno Nacional

// COMMAND ----------

import com.mongodb.spark._
import org.apache.spark.sql.functions._

dfSentimentAnalysisFull
  .select($"tweet_mentions_user",$"tweet_filter",$"polaridad")
  .withColumn("tweet_mentions_user",explode($"tweet_mentions_user"))
  .withColumn("words",explode(split($"tweet_filter"," ")))
  .filter($"polaridad" < 0 && ($"tweet_mentions_user" isin ("infopresidencia","FiscaliaCol","PGN_COL","mindefensa","MinInterior","IvanDuque")))
  .filter(isNumber($"words") === "False" && !$"words".contains("covid") && $"words" =!= "cc")
  .groupBy($"words")
  .count()
  .write
  .format("com.mongodb.spark.sql.DefaultSource")//Output data source
  .mode("overwrite")//Se remplaza el set de datos
  .option("database", "twitter")//Base de datos
  .option("collection", "words_neg_presidencia")//Collection
  .save()//Guarda el resultado

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC dfWordsNegPresidencia = (spark
// MAGIC       .read
// MAGIC       .format("com.mongodb.spark.sql.DefaultSource")
// MAGIC       .option("database", "twitter")
// MAGIC       .option("collection", "words_neg_presidencia")
// MAGIC       .load()
// MAGIC       .drop("_id")
// MAGIC       .repartition(8)
// MAGIC       .cache()
// MAGIC       )
// MAGIC 
// MAGIC print("Cantidad de registros cargados = "+str(dfWordsNegPresidencia.count()))
// MAGIC 
// MAGIC wordsNegPresidencia = dfWordsNegPresidencia.toPandas()
// MAGIC 
// MAGIC wordsNegPresidencia.set_index('words',inplace=True)

// COMMAND ----------

// MAGIC %md
// MAGIC Las palabras mas mencionadas tienen relacion con los siguientes aspectos:
// MAGIC - Casos de corrupcion entorno a las ayudas entregadas por el gobierno
// MAGIC - Gastos que ha incurrido el gobierno nacional pero no son necesarios en medio de la epidemia
// MAGIC - Condiciones de vulnerabilidad de algunas familias

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC from wordcloud import WordCloud
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC textWords = wordsNegPresidencia.to_dict("dict")["count"]
// MAGIC 
// MAGIC wordcloud = WordCloud(width = 1500, height = 1500, background_color ='white',min_font_size = 10,max_words=100,prefer_horizontal=1)
// MAGIC 
// MAGIC wordcloud.generate_from_frequencies(textWords) 
// MAGIC 
// MAGIC fig, ax = plt.subplots(figsize=(15,15))
// MAGIC plt.imshow(wordcloud)
// MAGIC plt.axis("off")
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 40.</b> Se grafica las palabras mas utilizadas dentro del Tweets con sentimiento negativo, relacionadas con el Gobierno Distrital de Bogotá

// COMMAND ----------

import com.mongodb.spark._
import org.apache.spark.sql.functions._

dfSentimentAnalysisFull
  .select($"tweet_mentions_user",$"tweet_filter",$"polaridad")
  .withColumn("tweet_mentions_user",explode($"tweet_mentions_user"))
  .withColumn("words",explode(split($"tweet_filter"," ")))
  .filter($"polaridad" < 0 && ($"tweet_mentions_user" isin ("Bogota","ClaudiaLopez")))
  .filter(isNumber($"words") === "False" && !$"words".contains("covid"))
  .groupBy($"words")
  .count()
  .write
  .format("com.mongodb.spark.sql.DefaultSource")//Output data source
  .mode("overwrite")//Se remplaza el set de datos
  .option("database", "twitter")//Base de datos
  .option("collection", "words_neg_bogota")//Collection
  .save()//Guarda el resultado

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC dfWordsNegBogota = (spark
// MAGIC       .read
// MAGIC       .format("com.mongodb.spark.sql.DefaultSource")
// MAGIC       .option("database", "twitter")
// MAGIC       .option("collection", "words_neg_bogota")
// MAGIC       .load()
// MAGIC       .drop("_id")
// MAGIC       .repartition(8)
// MAGIC       .cache()
// MAGIC       )
// MAGIC 
// MAGIC print("Cantidad de registros cargados = "+str(dfWordsNegBogota.count()))
// MAGIC 
// MAGIC wordsNegBogota = dfWordsNegBogota.toPandas()
// MAGIC 
// MAGIC wordsNegBogota.set_index('words',inplace=True)

// COMMAND ----------

// MAGIC %md
// MAGIC Las palabras mas mencionadas tienen relacion con los siguientes aspectos:
// MAGIC - Concentracion de la epidemia en ciertas localidades de Bogotá
// MAGIC - Manejo que ha dado la alcaldesa a la epidemia
// MAGIC - Caso de corrupcion por el desvio de dineros en el concejo de Bogotá

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC from wordcloud import WordCloud
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC textWords = wordsNegBogota.to_dict("dict")["count"]
// MAGIC 
// MAGIC wordcloud = WordCloud(width = 1500, height = 1500, background_color ='white',min_font_size = 10,max_words=100,prefer_horizontal=1)
// MAGIC 
// MAGIC wordcloud.generate_from_frequencies(textWords) 
// MAGIC 
// MAGIC fig, ax = plt.subplots(figsize=(15,15))
// MAGIC plt.imshow(wordcloud)
// MAGIC plt.axis("off")
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 41.</b> Se grafica las palabras mas utilizadas dentro del Tweets con sentimiento negativo, relacionadas con el usuario omarbula

// COMMAND ----------

import com.mongodb.spark._
import org.apache.spark.sql.functions._

dfSentimentAnalysisFull
  .select($"tweet_mentions_user",$"tweet_filter",$"polaridad")
  .withColumn("tweet_mentions_user",explode($"tweet_mentions_user"))
  .withColumn("words",explode(split($"tweet_filter"," ")))
  .filter($"polaridad" < 0 && $"tweet_mentions_user" === "omarbula")
  .filter(isNumber($"words") === "False" && !$"words".contains("covid"))
  .groupBy($"words")
  .count()
  .write
  .format("com.mongodb.spark.sql.DefaultSource")//Output data source
  .mode("overwrite")//Se remplaza el set de datos
  .option("database", "twitter")//Base de datos
  .option("collection", "words_neg_usuario")//Collection
  .save()//Guarda el resultado

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC dfWordsNegUsuario = (spark
// MAGIC       .read
// MAGIC       .format("com.mongodb.spark.sql.DefaultSource")
// MAGIC       .option("database", "twitter")
// MAGIC       .option("collection", "words_neg_usuario")
// MAGIC       .load()
// MAGIC       .drop("_id")
// MAGIC       .repartition(8)
// MAGIC       .cache()
// MAGIC       )
// MAGIC 
// MAGIC print("Cantidad de registros cargados = "+str(dfWordsNegUsuario.count()))
// MAGIC 
// MAGIC wordsNegUsuario = dfWordsNegUsuario.toPandas()
// MAGIC 
// MAGIC wordsNegUsuario.set_index('words',inplace=True)

// COMMAND ----------

// MAGIC %md
// MAGIC Las palabras mas mencionadas tienen relacion con el manejo que se ha dado de la epidemia en Bogota, haciendo enfasis que el autor de los Tweets es contradictor de ideales politcos de la mandataria

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC from wordcloud import WordCloud
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC textWords = wordsNegUsuario.to_dict("dict")["count"]
// MAGIC 
// MAGIC wordcloud = WordCloud(width = 1500, height = 1500, background_color ='white',min_font_size = 10,max_words=100,prefer_horizontal=1)
// MAGIC 
// MAGIC wordcloud.generate_from_frequencies(textWords) 
// MAGIC 
// MAGIC fig, ax = plt.subplots(figsize=(15,15))
// MAGIC plt.imshow(wordcloud)
// MAGIC plt.axis("off")
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 42.</b> Grafico de los usuarios mas mencionados en el periodo de analisis cuyo Tweet tiene sentimiento positivo:
// MAGIC - Cuentas de Twitter referentes al Gobierno Nacional

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfPositiva21 = filterDFDayPolaridadPositiva(dfSentimentAnalysisFull,21)
val dfPositiva22 = filterDFDayPolaridadPositiva(dfSentimentAnalysisFull,22)
val dfPositiva23 = filterDFDayPolaridadPositiva(dfSentimentAnalysisFull,23)
val dfPositiva24 = filterDFDayPolaridadPositiva(dfSentimentAnalysisFull,24)
val dfPositiva25 = filterDFDayPolaridadPositiva(dfSentimentAnalysisFull,25)
val dfPositiva26 = filterDFDayPolaridadPositiva(dfSentimentAnalysisFull,26)

val dfPositiva = dfPositiva21
  .unionAll(dfPositiva22)
  .unionAll(dfPositiva23)
  .unionAll(dfPositiva24)
  .unionAll(dfPositiva25)
  .unionAll(dfPositiva26)

display(dfPositiva)

// COMMAND ----------

// MAGIC %md
// MAGIC <b>Paso 43.</b> Se grafica las palabras mas utilizadas dentro del Tweets con sentimiento positivo, relacionadas con las cuentas del Gobierno Nacional

// COMMAND ----------

import com.mongodb.spark._
import org.apache.spark.sql.functions._

dfSentimentAnalysisFull
  .select($"tweet_mentions_user",$"tweet_filter",$"polaridad")
  .withColumn("tweet_mentions_user",explode($"tweet_mentions_user"))
  .withColumn("words",explode(split($"tweet_filter"," ")))
  .filter($"polaridad" > 0 && ($"tweet_mentions_user" isin ("infopresidencia","MinAgricultura","FiscaliaCol","MinjusticiaCo","MinInterior","MintrabajoCol","mindefensa","IvanDuque","mluciaramirez")))
  .filter(isNumber($"words") === "False" && !$"words".contains("covid"))
  .groupBy($"words")
  .count()
  .write
  .format("com.mongodb.spark.sql.DefaultSource")//Output data source
  .mode("overwrite")//Se remplaza el set de datos
  .option("database", "twitter")//Base de datos
  .option("collection", "words_pos_presidencia")//Collection
  .save()//Guarda el resultado

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC dfWordsPosPresidencia = (spark
// MAGIC       .read
// MAGIC       .format("com.mongodb.spark.sql.DefaultSource")
// MAGIC       .option("database", "twitter")
// MAGIC       .option("collection", "words_pos_presidencia")
// MAGIC       .load()
// MAGIC       .drop("_id")
// MAGIC       .repartition(8)
// MAGIC       .cache()
// MAGIC       )
// MAGIC 
// MAGIC print("Cantidad de registros cargados = "+str(dfWordsPosPresidencia.count()))
// MAGIC 
// MAGIC wordsPosPresidencia = dfWordsPosPresidencia.toPandas()
// MAGIC 
// MAGIC wordsPosPresidencia.set_index('words',inplace=True)

// COMMAND ----------

// MAGIC %md
// MAGIC Las palabras mas mencionadas tienen relacion con los siguientes aspectos:
// MAGIC - Medidas de prevencion y accion en contra de la epidemia
// MAGIC - Balance de las ayudas sociales que ha dado el gobiernos a la poblacion mas vulnerable
// MAGIC - Medidas de proteccion economica

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC from wordcloud import WordCloud
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC textWords = wordsPosPresidencia.to_dict("dict")["count"]
// MAGIC 
// MAGIC wordcloud = WordCloud(width = 1500, height = 1500, background_color ='white',min_font_size = 10,max_words=100,prefer_horizontal=1)
// MAGIC 
// MAGIC wordcloud.generate_from_frequencies(textWords) 
// MAGIC 
// MAGIC fig, ax = plt.subplots(figsize=(15,15))
// MAGIC plt.imshow(wordcloud)
// MAGIC plt.axis("off")
// MAGIC display(fig)
