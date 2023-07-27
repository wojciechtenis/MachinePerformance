package oeecalculator.write

import org.apache.spark.sql.DataFrame

trait StreamWriter {

  def write(data: DataFrame): Unit

}
