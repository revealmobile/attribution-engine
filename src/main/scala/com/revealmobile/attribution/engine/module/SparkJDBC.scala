package com.revealmobile.attribution.engine.module

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkJDBC(url:String, props:Properties) {

  def loadQuery(spark: SparkSession, query: String): DataFrame = {
    spark.read.jdbc(url, s"($query) AS aliased", props)
  }

  def loadTable(spark: SparkSession, table: String): DataFrame = {
    spark.read.jdbc(url, table, props)
  }

  def appendToTable(spark: SparkSession, df: DataFrame, table: String): Unit = {
    df.write.mode("append").jdbc(url, table, props)
  }

}
