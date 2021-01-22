package com.revealmobile.attribution.engine.repository

import javax.inject.Inject
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PixelDeviceRepository  @Inject()(spark: SparkSession) {

  val pixelDeviceSchema = StructType(Array(
    StructField("timestamp",TimestampType,true),
    StructField("id",StringType,true),
    StructField("type",StringType,true),
    StructField("guid", StringType, true)
  ))

  def find(guid: String): DataFrame = {
    val pixelLogDF = spark.read
      .option("header",true)
      .schema(pixelDeviceSchema)
      .csv(s"s3://reveal-attribution/logfiles/*")

    pixelLogDF
      .filter(pixelLogDF("guid").contains(guid))
      .select("id")
      .distinct()
      .toDF
  }



}
