package com.revealmobile.attribution.engine.repository

import javax.inject.Inject
import org.apache.spark.sql.{DataFrame, SparkSession}

class PixelExposedHouseholdDeviceRepository @Inject()(spark: SparkSession) {

  def count(sourceDeviceDF: DataFrame, householdDeviceDF: DataFrame): Long = {
    spark.sql("DROP TABLE IF EXISTS pixel_matched_conversion_zone_visitors_hh")
    sourceDeviceDF.write.format("parquet").saveAsTable("pixel_matched_conversion_zone_visitors_hh")

    spark.sql("DROP TABLE IF EXISTS local_household_device")
    householdDeviceDF
      .write
      .format("parquet")
      .saveAsTable("local_household_device")

    spark.sql("DROP TABLE IF EXISTS pixel_matched_conversion_zone_household_devices")
    spark.sql(
      """
         SELECT hh.maid, hh.other_maid, hh.other_os FROM local_household_device hh
         JOIN pixel_matched_conversion_zone_visitors_hh p ON p.id = hh.maid
        """
    ).write
      .format("parquet")
      .saveAsTable("pixel_matched_conversion_zone_household_devices")

    spark.sql("DROP TABLE IF EXISTS hh_enriched_pixel_matched_conversion_zone_devices")
    spark.sql(
      """
         SELECT id FROM pixel_matched_conversion_zone_visitors_hh p
         UNION
         SELECT hh.other_maid AS id
         FROM pixel_matched_conversion_zone_household_devices hh
      """
    )
      .distinct()
      .write
      .format("parquet")
      .saveAsTable("hh_enriched_pixel_matched_conversion_zone_devices")

    spark.table("hh_enriched_pixel_matched_conversion_zone_devices").count()
  }


}
