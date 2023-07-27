package oeecalculator.write

import org.apache.spark.sql.DataFrame

class ConsoleStreamWriter extends StreamWriter {

  override def write(data: DataFrame): Unit = {
    data.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }
}
