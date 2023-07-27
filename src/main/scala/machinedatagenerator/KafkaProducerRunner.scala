package machinedatagenerator

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KafkaProducerRunner {

  val props:Properties = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")

  def run(topicName: String, msg: String): Unit = {
    val producer = new KafkaProducer[String, String](props)
    val prodRecord = new ProducerRecord[String, String](topicName, msg)

    try {
      producer.send(prodRecord)
    }finally {
      producer.close()
    }

  }

}
