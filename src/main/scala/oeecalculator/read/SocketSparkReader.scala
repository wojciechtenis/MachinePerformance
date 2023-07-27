package oeecalculator.read
import org.apache.spark.sql.{DataFrame, SparkSession}

class SocketSparkReader(sparkSession: SparkSession) extends StreamReader {

  override def read(serverUrl: String, port: String): DataFrame = {
    sparkSession.readStream
      .format("socket")
      .option("host", serverUrl)
      .option("port", port)
      .load()
  }

}
