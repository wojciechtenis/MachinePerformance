import machinedatagenerator.MachineDataGenerator
import oeecalculator.OEECalculationRunner
import oeecalculator.spark.SparkUtils.createSparkSession

object OEECalculator {

  def main(args: Array[String]): Unit = {

    println("OEE Calculator Running...")
    val sparkSession = createSparkSession
    sparkSession.sparkContext.setLogLevel("ERROR")

    //---run oee calculation along with machine message sending simulation
    //-----run oee calculation
    val dataGeneratorThread = new Thread(new MachineDataGenerator)
    //-----simulate machine output kafka messages
    val oEECalculatorThread = new Thread(new OEECalculationRunner(sparkSession))
    oEECalculatorThread.getPriority
    oEECalculatorThread.start()
    Thread.sleep(10000)
    dataGeneratorThread.start()

  }



}
