package oeecalculator.read

import oeecalculator.conf.ReaderConfig
import org.apache.logging.log4j.core.config.ConfigurationException
import org.apache.spark.sql.SparkSession

class StreamReaderFactory(sparkSession: SparkSession) {

  def get(readerConfig: ReaderConfig): StreamReader = {
    readerConfig.readerType match {
      case "kafka" =>
        val topic = readerConfig.topicNameOpt.getOrElse(throw new ConfigurationException("Kafka stream reader has been defined but topic name is missing. Please add topic in the configuration file"))
        new KafkaStreamReader(sparkSession, topic)
      case "socket" => new SocketSparkReader(sparkSession)
      case _ => throw new ConfigurationException("No stream reader has been configured. Please define it in the configuration file")
    }
  }

}
