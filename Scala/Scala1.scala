// Databricks notebook source
// MAGIC %md
// MAGIC <h2>Lectura de Datos con Scala</h2> En esta primer sección aprenderemos a crear un Dataframe de Apache Spark referenciando un tipo de archivo <b>parquet</b>."

// COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/definitive-guide/data/flight-data"))

// COMMAND ----------

val ruta = "/databricks-datasets/definitive-guide/data/flight-data/parquet"
display(dbutils.fs.ls(ruta))


// COMMAND ----------

val ruta_info = "/databricks-datasets/definitive-guide/data/flight-data/parquet/2010-summary.parquet"
var flight = spark.read.parquet(ruta_info)

// COMMAND ----------

flight.show()

// COMMAND ----------

flight.filter($"count" < 100 ).show()

// COMMAND ----------

flight.filter($"count" < 100 ).count()

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

var df = flight.
        filter($"count" < 100).
        withColumnRenamed("DEST_COUNTRY_NAME","pais_destino").
        withColumnRenamed("ORIGIN_COUNTRY_NAME","pais_origen").
        withColumnRenamed("count","cantidad_vuelos")

// COMMAND ----------

df.count()

// COMMAND ----------

df.show(5)

// COMMAND ----------

df.filter($"pais_origen" === "United States").
    groupBy($"pais_destino").
                              agg(
                                sum($"cantidad_vuelos").alias("Cantidad")
                              ).show()

// COMMAND ----------

df.createOrReplaceTempView("reporte")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from reporte

// COMMAND ----------

// MAGIC %md
// MAGIC <h2>Subir y Lectura de Datos con Scala</h2> En esta segunda sección aprenderemos a subir archivos <b>para poder leerlos con apache spark</b>."

// COMMAND ----------

display(dbutils.fs.ls("FileStore/tables/"))

// COMMAND ----------

dbutils.fs.mkdirs("FileStore/tables/dataset/titanic")

// COMMAND ----------

display(dbutils.fs.ls("FileStore/tables/dataset/titanic"))

// COMMAND ----------

var titanic = spark.read.format("csv").
      option("header",true).option("delimiter",",").
      load("/FileStore/tables/dataset/titanic/titanic.csv")

// COMMAND ----------

titanic.show()

// COMMAND ----------

titanic.count()

// COMMAND ----------

// MAGIC %md
// MAGIC <h2>Estadística descriptiva</h2>

// COMMAND ----------

display(titanic.summary())

// COMMAND ----------

// MAGIC %md
// MAGIC <h2>Distribución de varias variables</h2>

// COMMAND ----------

titanic.createOrReplaceTempView("reporte_1")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT sex FROM reporte_1;
