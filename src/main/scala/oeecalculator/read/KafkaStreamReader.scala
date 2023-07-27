package oeecalculator.read

import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaStreamReader(sparkSession: SparkSession, topic: String) extends StreamReader {

  def read(serverUrl: String, port: String): DataFrame = {
    val fullServerUrl = s"$serverUrl:$port"

    sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", fullServerUrl)
      .option("subscribe",topic)
      .option("startingOffsets", "earliest")
      .load()
  }

}
