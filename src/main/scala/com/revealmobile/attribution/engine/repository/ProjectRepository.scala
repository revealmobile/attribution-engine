package com.revealmobile.attribution.engine.repository

import com.revealmobile.attribution.engine.model.project.ProjectPlaceRecord
import com.revealmobile.attribution.engine.module.SparkJDBC
import javax.inject.Inject
import org.apache.spark.sql.{Dataset, SparkSession}

class ProjectRepository @Inject()(
   spark: SparkSession,
   sparkJDBC: SparkJDBC
  ) {
 import spark.implicits._

  def find(projectId: Long): Dataset[ProjectPlaceRecord] = {
    val df = sparkJDBC
      .loadTable(spark, "auth.project")
      .withColumnRenamed("id", "projectId")
      .withColumnRenamed("chain_id", "chainId")
      .withColumnRenamed("place_ids", "placeIds")
      .withColumnRenamed("country_id", "countryIds")
      .filter($"projectId" === projectId)
      .select("projectId", "chainId", "placeIds", "countryIds")

      df.show(1)
      df.as[ProjectPlaceRecord]
  }

}
