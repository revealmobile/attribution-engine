package com.revealmobile.attribution.engine

import com.typesafe.config.Config
import java.time.LocalDate

import com.revealmobile.attribution.engine.model.project.ProjectPlaceRecord
import com.revealmobile.attribution.engine.model.temporal.LocalDateRange
import com.revealmobile.attribution.engine.repository.{
  ConversionZoneRepository,
  ConversionZoneVisitorRepository,
  DeviceRepository,
  HouseholdDeviceRepository,
  PixelExposedConversionZoneVisitorRepository,
  PixelExposedDeviceRepository,
  PixelExposedHouseholdDeviceRepository,
  PixelDeviceRepository,
  ProjectRepository
}
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

@Singleton
class PixelReportBuilder @Inject()(
  config: Config,
  conversionZoneRepository: ConversionZoneRepository,
  conversionZoneVisitorRepository: ConversionZoneVisitorRepository,
  pixelDeviceRepository: PixelDeviceRepository,
  pixelExposedDeviceRepository: PixelExposedDeviceRepository,
  pixelExposedHouseholdDeviceRepository: PixelExposedHouseholdDeviceRepository,
  pixelExposedConversionZoneVisitorRepository: PixelExposedConversionZoneVisitorRepository,
  deviceRepository: DeviceRepository,
  projectRepository: ProjectRepository,
  householdDeviceRepository: HouseholdDeviceRepository) {

  private val guid = config.getString("guid")
  private val projectId = config.getLong("project_id")
  private val conversionRange = LocalDateRange(
    LocalDate.parse(config.getString("start")),
    LocalDate.parse(config.getString("end"))
  )
  private val includeHouseholds = config.getBoolean("include_households")

  def reportTotalPixelExposedDevices(pixelDevices: DataFrame): Unit = {
    println(s"Total Pixel Exposed Devices: ${pixelDevices.count()} maids from tapad.")
  }

  def reportTotalPixelExposedConvertedDevices(
    projectPlaceRecord: ProjectPlaceRecord,
    range: LocalDateRange,
    pixelDevices: DataFrame,
    households: Option[DataFrame] = None): Unit = {
    val placeIds = projectPlaceRecord.placeIds
    val chainIds = Set(projectPlaceRecord.chainId).flatten
    val countryIds = projectPlaceRecord.countryIds.getOrElse(Set.empty)
    val conversionZones = conversionZoneRepository.find(placeIds, chainIds, countryIds)

    val conversionsDF = conversionZoneVisitorRepository.find(conversionZones, range)
    val pixelConverterCount = pixelExposedConversionZoneVisitorRepository.count(conversionsDF, pixelDevices)
    println(s"Pixel Converter Report: ${pixelConverterCount} devices exposed to pixel and visited conversion zone.")
    //optional household output
    households.foreach { householdDeviceDF =>
      val pixelConverterHHEnrichedDF = pixelExposedConversionZoneVisitorRepository.find(conversionsDF, pixelDevices)
      val pixelMatchedHHEnrichedCount = pixelExposedHouseholdDeviceRepository.count(pixelConverterHHEnrichedDF, householdDeviceDF)
      println(s"HH enriched pixel matched conversion zone: ${pixelMatchedHHEnrichedCount}")
    }
  }

  def reportAllPixelMatchedDevices(allDevices: DataFrame,
    pixelDevices: DataFrame,
    households: Option[DataFrame]): Unit = {
    val allMatchedDevicesCount = pixelExposedDeviceRepository.count(allDevices, pixelDevices)
    println(s"Pixel Matched Devices Report: ${allMatchedDevicesCount} devices.")
    //optional household output
    households.foreach { householdDeviceDF =>
      val matchedDevicesDF = pixelExposedDeviceRepository.find(allDevices, pixelDevices)
      val allDevicesPixelMatchedHHEnrichedCount = pixelExposedHouseholdDeviceRepository.count(matchedDevicesDF, householdDeviceDF)
      println(s"HH enriched all matched pixel devices in past year: ${allDevicesPixelMatchedHHEnrichedCount}")
    }
  }

  def build: Unit = {
    // lookup project places to define conversion zone geometries
    val projectPlaceRecord = projectRepository.find(projectId).head()
    // lookup pixel logs for campaign
    val pixelDevicesDF = pixelDeviceRepository.find(guid)
    // lookup all reveal maids for matching
    val allDevicesDF = deviceRepository.list
    // lookup household devices for enrichment if requested
    val optionalHouseholdDeviceDF = if (includeHouseholds) {
      Option(householdDeviceRepository.list)
    } else {
      None
    }
    // generate reports
    reportTotalPixelExposedDevices(pixelDevicesDF)
    reportAllPixelMatchedDevices(allDevices = allDevicesDF,
      pixelDevices = pixelDevicesDF,
      households = optionalHouseholdDeviceDF
    )
    reportTotalPixelExposedConvertedDevices(
      projectPlaceRecord = projectPlaceRecord,
      range = conversionRange,
      pixelDevices = pixelDevicesDF,
      households = optionalHouseholdDeviceDF
    )
  }

}
