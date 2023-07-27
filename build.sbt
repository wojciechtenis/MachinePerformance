name := "MachinePerformance"

version := "0.1"

scalaVersion := "2.13.11"
val sparkVersion = "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.4.0"
libraryDependencies += "log4j" % "log4j" % "1.2.16"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.2"
libraryDependencies += "joda-time" % "joda-time" % "2.12.5"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.4.0"