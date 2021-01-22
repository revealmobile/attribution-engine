package com.revealmobile.attribution.engine.repository

import com.revealmobile.attribution.engine.model.attribution.ConversionZone
import javax.inject.Inject
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class PixelExposedConversionZoneVisitorRepository @Inject()(spark: SparkSession) {

  import spark.implicits._

  def find(conversionsDF: DataFrame, pixelVisitorsDF: DataFrame): DataFrame = {
    spark.sql("DROP TABLE IF EXISTS unmatched_pixel_visitors")
    pixelVisitorsDF.write.format("parquet").saveAsTable("unmatched_pixel_visitors")

    spark.sql("DROP TABLE IF EXISTS conversion_zone_visitors")
    conversionsDF.write.saveAsTable("conversion_zone_visitors")

    spark.sql("DROP TABLE IF EXISTS pixel_matched_conversion_zone_visitors")
    spark.sql(
      """
         SELECT DISTINCT p.id
         FROM unmatched_pixel_visitors p
         INNER JOIN conversion_zone_visitors c
           ON p.id = c.advertiser_id
      """
    ).write
      .format("parquet")
      .saveAsTable("pixel_matched_conversion_zone_visitors")

    spark.table("pixel_matched_conversion_zone_visitors")
  }

  def count(conversionsDF: DataFrame, pixelVisitorsDF: DataFrame): Long = {
    find(conversionsDF, pixelVisitorsDF).count()
  }


}
