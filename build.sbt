name := "SparkStructuredStreaming"

version := "0.1"

scalaVersion := "2.11.8"

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.3.4",
    "org.apache.spark" % "spark-sql_2.11" % "2.3.4",
    "org.apache.spark" % "spark-streaming_2.11" % "2.3.4",
    "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.4" excludeAll excludeJpountz,
    "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "6.2.4"
)
