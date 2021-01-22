package com.revealmobile.attribution.engine.repository

import com.revealmobile.attribution.engine.model.attribution.ConversionZone
import javax.inject.Inject
import org.apache.spark.sql.{Dataset, SparkSession}

class ConversionZoneRepository @Inject()(spark: SparkSession) {

  import spark.implicits._

  def find(
    placeIds: Set[Long] = Set.empty,
    chainIds: Set[Long] = Set.empty,
    countryIds: Set[String] = Set.empty): Dataset[ConversionZone] = {

    spark.sql("DROP TABLE IF EXISTS point_of_interest")
    spark.sql(
      f"""
      CREATE EXTERNAL TABLE point_of_interest (
        id bigint,
        polygon_id bigint,
        name string,
        external_key string,
        poi_id bigint,
        chain_id bigint,
        lat double,
        lon double,
        address string,
        city string,
        state string,
        postal_code string,
        naics string,
        country string,
        org_id bigint
      )
      PARTITIONED BY (source string)
      STORED AS PARQUET
      LOCATION 's3://reveal-spark/parquet/point_of_interest'
      """)
    spark.sql("ALTER TABLE point_of_interest RECOVER PARTITIONS")

    val placeFilter = placeIds.headOption.map(_ => s"p.id IN (${placeIds.mkString(",")})")
    val chainFilter = chainIds.headOption.map(_ => s"p.chain_id IN (${chainIds.mkString(",")})")
    val countryFilter = countryIds.headOption.map(_ => s"p.country IN (${countryIds.mkString(",")})")

    val filters = Seq( placeFilter, chainFilter, countryFilter)
      .flatten

    val whereClause = filters.headOption.map(_ => s"WHERE ${filters.mkString(" AND ")}")

    val query = s"""
      SELECT id AS placeId, polygon_id AS polygonId
      FROM point_of_interest p
      ${whereClause.getOrElse("")}
      """

    spark.sql(query).as[ConversionZone]
  }
}
