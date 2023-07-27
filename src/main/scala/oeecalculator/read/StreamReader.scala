package oeecalculator.read

import org.apache.spark.sql.DataFrame

trait StreamReader {

  def read(serverUrl: String, port: String): DataFrame

}
