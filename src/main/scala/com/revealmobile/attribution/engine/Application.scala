package com.revealmobile.attribution.engine

import com.google.inject.Guice
import com.revealmobile.attribution.engine.module.table.HouseholdDeviceTableModule
import com.revealmobile.attribution.engine.util.ArgsUtil
import com.revealmobile.attribution.engine.module.{ConfigurationModule, SparkJDBCModule, SparkModule}
import net.codingwell.scalaguice.InjectorExtensions._


object Application {

    def main(args: Array[String]): Unit = {
      val parsedArgs = ArgsUtil.pixelAttributionReportArgs(args)
      val jobId = "pixel-attributon-report"

      val sparkSession = new SparkModule()

      val injector = Guice.createInjector (
        new ConfigurationModule(parsedArgs),
        new SparkModule(),
        new SparkJDBCModule(jobId),
        new HouseholdDeviceTableModule()
      )

      val builder = injector.instance[PixelReportBuilder]
      builder.build


    }
}
