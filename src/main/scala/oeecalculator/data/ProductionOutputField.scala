package oeecalculator.data

import org.apache.spark.sql.types.DataType

case class ProductionOutputField(id: Integer, name: String, dataType: DataType)
