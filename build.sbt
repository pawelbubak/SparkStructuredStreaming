name := "SparkStructuredStreaming"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.3.1",
    "org.apache.spark" % "spark-sql_2.11" % "2.3.1",
    "org.apache.spark" % "spark-streaming_2.11" % "2.3.1",
    "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.1"
)
