package com.revealmobile.attribution.engine.repository

import javax.inject.Inject
import org.apache.spark.sql.{DataFrame, SparkSession}

class PixelExposedDeviceRepository @Inject()(spark: SparkSession) {
  import org.apache.spark.sql.functions.broadcast

  def find(allDevicesDF: DataFrame, pixelVisitorsDF: DataFrame): DataFrame = {
    spark.sql("DROP TABLE IF EXISTS unmatched_pixel_visitors")
    broadcast(pixelVisitorsDF).createOrReplaceTempView("unmatched_pixel_visitors")

    allDevicesDF.createOrReplaceTempView("all_devices")

    spark.sql("DROP TABLE IF EXISTS all_matched_devices")
    spark.sql(
      """
         SELECT DISTINCT p.id
         FROM unmatched_pixel_visitors p
         INNER JOIN all_devices a
           ON p.id = a.advertiser_id
      """
    ).write
      .format("parquet")
      .saveAsTable("all_matched_devices")


    spark.table("all_matched_devices")
  }

  def count(allDevicesDF: DataFrame, pixelVisitorsDF: DataFrame): Long = {
    find(allDevicesDF, pixelVisitorsDF).count()
  }


}
