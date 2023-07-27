package oeecalculator

import constants.Constants._
import oeecalculator.data.ProductionOutputField
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame}
import oeecalculator.spark.SparkUdfUtils

class DataProcessor(dataFieldsList: Seq[ProductionOutputField]) {

  def process(data: DataFrame): DataFrame = {
    val structuredData = getStructuredData(data, dataFieldsList)
    calculateOEEPerShift(structuredData)
  }

  private def getColumnFormArray(field: ProductionOutputField): Column = element_at(col("data"), field.id).cast(field.dataType).as(field.name)

  private def getStructuredData(rawData: DataFrame, fieldsList: Seq[ProductionOutputField]): DataFrame = {
    val columnsFormArrayCol = fieldsList.foldLeft(Seq[Column]())((columns, field) => columns :+ getColumnFormArray(field))
    rawData.select(split(col("value"), "\\|").as("data")).select(columnsFormArrayCol :_*)
  }

  private def calculateOEEPerShift(streamData: DataFrame): DataFrame = {
    val aggregatedDataPerShift = streamData.select(PROD_TIMESTAMP_COLUMN_NAME, VOLUME_COLUMN_NAME)
      .withColumn(SHIFT_START_TIMESTAMP_COLUMN_NAME, SparkUdfUtils.calculateShiftStartTimeUdf(col(PROD_TIMESTAMP_COLUMN_NAME)))
      .withWatermark(SHIFT_START_TIMESTAMP_COLUMN_NAME, "8 hours")
      .groupBy(window(col(SHIFT_START_TIMESTAMP_COLUMN_NAME), "8 hour", "8 hour", "4 hour"))
      .agg(sum(VOLUME_COLUMN_NAME).as(TOTAL_PROD_COLUMN_NAME), max(PROD_TIMESTAMP_COLUMN_NAME).as(LAST_PROD_TIMESTAMP_COLUMN_NAME))

    aggregatedDataPerShift.select("window.start", "window.end", LAST_PROD_TIMESTAMP_COLUMN_NAME, TOTAL_PROD_COLUMN_NAME)
      .withColumn(NOMINAL_SPEED_COLUMN_NAME, lit(100))
      .withColumn(RUNNING_TIME_COLUMN_NAME, (col(LAST_PROD_TIMESTAMP_COLUMN_NAME).cast(LongType) - col("start").cast(LongType)) / 60)
      .withColumn(OEE_COLUMN_NAME, round(col(TOTAL_PROD_COLUMN_NAME) / (col(RUNNING_TIME_COLUMN_NAME) * col(NOMINAL_SPEED_COLUMN_NAME)) * 100, 2))
  }

}
