package com.revealmobile.attribution.engine.util

import java.time.{LocalDate, ZoneOffset}
import java.time.temporal.ChronoUnit.DAYS

import com.revealmobile.attribution.engine.model.temporal.LocalDateRange


object DateUtil {

  def today: LocalDate = {
    LocalDate.now(ZoneOffset.UTC)
  }

  def getDatesBetween(startDate: LocalDate, endDate: LocalDate): Seq[LocalDate] = {
    val days = DAYS.between(startDate, endDate)
    for (f <- 0L to days) yield startDate.plusDays(f)
  }

  def overlap(rangeA: LocalDateRange, rangeB: LocalDateRange): Boolean = {
   getDatesBetween(rangeA.from, rangeA.to).intersect(getDatesBetween(rangeB.from, rangeB.to)).nonEmpty
  }

  def consecutive(rangeA: LocalDateRange, rangeB: LocalDateRange): Boolean = {
    DAYS.between(rangeA.to, rangeB.from) == 1 || DAYS.between(rangeB.to, rangeA.from) == 1
  }

}
