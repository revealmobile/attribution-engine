package com.revealmobile.attribution.engine.repository

import com.revealmobile.attribution.engine.module.table.HouseholdDeviceTable
import javax.inject.Inject
import org.apache.spark.sql.{DataFrame, SparkSession}

class HouseholdDeviceRepository @Inject()(
  spark: SparkSession,
  householdDeviceTable: HouseholdDeviceTable
) {

  def list: DataFrame = {
    spark.table("household_device_exploded")
  }

}
