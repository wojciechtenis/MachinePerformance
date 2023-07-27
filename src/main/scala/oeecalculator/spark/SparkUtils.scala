package oeecalculator.spark

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def createSparkSession: SparkSession = {
    SparkSession.builder()
      .appName("OEE Calculator")
      .config("oeecalculator.spark.sql.shuffle.partitions", 2)
      .master("local[*]")
      .getOrCreate()
  }

}
