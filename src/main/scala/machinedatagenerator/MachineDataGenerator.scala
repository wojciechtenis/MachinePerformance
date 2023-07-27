package machinedatagenerator

import org.joda.time.DateTime

class MachineDataGenerator extends Runnable {

  def run(): Unit = {
    var startTime = new DateTime(2023, 7, 26, 6, 1, 0)

    while(!startTime.equals(new DateTime(2023, 7, 30, 14, 0, 0))) {
      val msg = generateMessage(startTime)
      KafkaProducerRunner.run("machineOutput", msg)
      println(s"Kafka message sent: $msg")
      Thread.sleep(10000)
      startTime = startTime.plusMinutes(1)
    }
  }

  private def generateMessage(timestampVal: DateTime): String = {

    val machineID = 11
    val volume = generateVolumne()
    val timestamp = timestampVal
    val productType= "can"
    val productId = "33311231"

    Seq(machineID, volume, timestamp, productType, productId).mkString("|")
  }

//machine output simulation
  private def generateVolumne(): Int = {
    Math.round((Math.random()*60)+40).toInt

  }

}
