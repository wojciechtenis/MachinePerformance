package oeecalculator

import oeecalculator.conf.{ReaderConfig, WriterConfig}
import oeecalculator.data.{ProductionDataSchema, ProductionOutputField}
import org.apache.spark.sql.{DataFrame, SparkSession}
import oeecalculator.read.StreamReaderFactory
import oeecalculator.write.StreamWriterFactory

class OEECalculationRunner(sparkSession: SparkSession, dataFields: Seq[ProductionOutputField] = ProductionDataSchema.getListOfFields) extends Runnable {

  def run(): Unit = {
    val readerConf = ReaderConfig("kafka", "localhost", "9092", Some("machineOutput"))
    val streamDataRaw = readStreamData(sparkSession, readerConf)

    val dataProcessor = new DataProcessor(dataFields)
    val aggregatedDataPerShift = dataProcessor.process(streamDataRaw)

    val writerConf = WriterConfig("console")
    writeStreamData(aggregatedDataPerShift, writerConf)
  }

  private def readStreamData(sparkSession: SparkSession, readerConfig: ReaderConfig): DataFrame = {
    val streamReader = new StreamReaderFactory(sparkSession).get(readerConfig)
    streamReader.read(readerConfig.serverUrl, readerConfig.port)
  }

  private def writeStreamData(data: DataFrame, writerConfig: WriterConfig): Unit = {
    val streamWriter = new StreamWriterFactory().get(writerConfig)
    streamWriter.write(data)
  }

}
