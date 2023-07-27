package oeecalculator.data

import constants.Constants.{MACHINE_ID_COLUMN_NAME, PRODUCT_ID_COLUMN_NAME, PRODUCT_TYPE_COLUMN_NAME, PROD_TIMESTAMP_COLUMN_NAME, VOLUME_COLUMN_NAME}
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}

object ProductionDataSchema {


  def getListOfFields: Seq[ProductionOutputField] = {
    Seq(
      ProductionOutputField(1, MACHINE_ID_COLUMN_NAME, IntegerType),
      ProductionOutputField(2, VOLUME_COLUMN_NAME, IntegerType),
      ProductionOutputField(3, PROD_TIMESTAMP_COLUMN_NAME, TimestampType),
      ProductionOutputField(4, PRODUCT_TYPE_COLUMN_NAME, StringType),
      ProductionOutputField(5, PRODUCT_ID_COLUMN_NAME, StringType)
    )
  }

}
