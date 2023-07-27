package oeecalculator.conf

case class ReaderConfig(readerType: String, serverUrl: String, port: String, topicNameOpt: Option[String])
