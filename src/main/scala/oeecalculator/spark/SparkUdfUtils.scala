package oeecalculator.spark

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.joda.time.DateTime

import java.sql.Timestamp

object SparkUdfUtils {


  val calculateShiftStartTime: Timestamp => Timestamp = (timestamp: Timestamp) => {

    val dateTime = timestamp.toLocalDateTime

    timestamp.toLocalDateTime
    val year = dateTime.getYear
    val month = dateTime.getMonth.getValue
    val day = dateTime.getDayOfMonth
    val hours = dateTime.getHour
    val minutes = dateTime.getMinute

    val shiftStartDateTime = if (((hours == 6 && minutes > 0) || (hours > 6)) && (hours < 14 || (hours == 14 && minutes == 0))) {
      new DateTime(year, month, day, 6, 0, 0)
    } else if (((hours == 14 && minutes > 0) || (hours > 14)) && (hours < 22 || (hours == 22 && minutes == 0))) {
      new DateTime(year, month, day, 14, 0, 0)
    } else if ((hours == 22 && minutes > 0) || hours > 22) {
      new DateTime(year, month, day, 22, 0, 0)
    } else new DateTime(year, month, day - 1, 22, 0, 0)

    Timestamp.valueOf(shiftStartDateTime.toString("yyyy-MM-dd HH:mm:ss"))
  }

  val calculateShiftStartTimeUdf: UserDefinedFunction = udf(calculateShiftStartTime)


}
