package oeecalculator.write

import oeecalculator.conf.WriterConfig
import org.apache.logging.log4j.core.config.ConfigurationException

class StreamWriterFactory {

  def get(writerConfig: WriterConfig): StreamWriter = {
    writerConfig.writerType match {
      case "console" => new ConsoleStreamWriter
      case _ => throw new ConfigurationException("No stream writer has been configured. Please define it in the configuration file")
    }
  }

}
