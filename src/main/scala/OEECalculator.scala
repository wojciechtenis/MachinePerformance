import machinedatagenerator.MachineDataGenerator
import oeecalculator.OEECalculationRunner
import oeecalculator.spark.SparkUtils.createSparkSession

object OEECalculator {

  def main(args: Array[String]): Unit = {

    println("OEE Calculator Running...")
    val sparkSession = createSparkSession
    sparkSession.sparkContext.setLogLevel("ERROR")

    //run oee calculation and simulation of sending messages by the machine in parallel
    val dataGeneratorThread = new Thread(new MachineDataGenerator)
    val oEECalculatorThread = new Thread(new OEECalculationRunner(sparkSession))
    oEECalculatorThread.getPriority
    oEECalculatorThread.start()
    Thread.sleep(10000)
    dataGeneratorThread.start()

  }



}
