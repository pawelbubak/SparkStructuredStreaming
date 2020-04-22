package com.bigdata.spark

import com.bigdata.spark.model.{Station, TripEvent}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkApplication {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Not enough arguments. " +
        "Provide D (minutes), P (ratio of the number of rentals/returns to the size of the station) for anomaly detection.")
      System.exit(0)
    }
    val d = args(0).toInt // 60
    val p = args(1).toInt // 50

    val spark = SparkSession.builder
      .master("local")
      .appName("ChicagoDivvyBicycleSharingData")
      .getOrCreate()

    import spark.implicits._

    val stations: Dataset[Station] = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("delimiter", ",")
      .load("project-data/stations/Divvy_Bicycle_Stations.csv")
      .withColumnRenamed("ID", "id")
      .withColumnRenamed("Station Name", "name")
      .withColumnRenamed("Total Docks", "totalDocks")
      .withColumnRenamed("Docks in Service", "docksInService")
      .withColumnRenamed("Status", "status")
      .withColumnRenamed("Latitude", "latitude")
      .withColumnRenamed("Longitude", "longitude")
      .withColumnRenamed("Location", "location")
      .as[Station]

    val messagesStream: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "bicycles-input-9")
      .load()

    val tripsDS: Dataset[TripEvent] = messagesStream
      .select($"key".cast(StringType), $"value".cast(StringType)).as[(String, String)]
      .filter(!_._2.startsWith("trip_id"))
      .map(_._2.split(","))
      .map(array => TripEvent(array(0).toInt, array(1).toInt, array(2), array(3).toInt,
        array(4).toDouble, array(5), array(6), array(7).toInt, array(8).toDouble, array(9)))

    val trips: DataFrame = tripsDS.select(to_timestamp($"eventTime", "yyyy-MM-dd'T'HH:mm:ss.SSSX").as("ts"),
      $"stationId", $"eventType", $"temperature")

    val result: DataFrame = trips
      .groupBy(
        window($"ts", "1 hour", "5 minutes"),
        $"stationId",
        date_trunc("Day", $"ts").as("date"))
      .agg(
        sum(when($"eventType" === 0, 1).otherwise(0)).as("departuresNumber"),
        sum(when($"eventType" === 1, 1).otherwise(0)).as("arrivalsNumber"),
        count($"temperature").as("count"),
        sum($"temperature").as("sum"))
      .withColumn("averageTemperature", $"sum" / $"count")

    val resultAsKeyValue: DataFrame = result
      .select(
        to_json(struct(
          date_format($"date", "yyyy-MM-dd").as("date"),
          $"stationId",
          $"departuresNumber",
          $"arrivalsNumber",
          $"averageTemperature"
        )).alias("value"))
      .withColumn("key", hash($"value"))
      .select($"key".cast(StringType), $"value".cast(StringType))

    val queryEtl: StreamingQuery = resultAsKeyValue.writeStream
      .outputMode(OutputMode.Update())
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "bicycles-output-9")
      .option("checkpointLocation", "/tmp/bicycles-output-9")
      .start()

    val anomalies: DataFrame = trips.
      groupBy(
        $"stationId",
        window($"ts", s"$d minutes", "5 minutes"))
      .agg(
        sum(when($"eventType" === 0, 1).otherwise(0)).as("departuresNumber"),
        sum(when($"eventType" === 1, 1).otherwise(0)).as("arrivalsNumber"))
      .join(stations, trips("stationId") === stations("id"))
      .select("window.start", "window.end", "name",
        "docksInService", "arrivalsNumber", "departuresNumber")
      .withColumn("uncompensatedReturnsNumber", $"arrivalsNumber" - $"departuresNumber")
      .withColumn("uncompensatedRentalsNumber", $"departuresNumber" - $"arrivalsNumber")
      .withColumn("nToDockRatio", when($"uncompensatedReturnsNumber" > $"uncompensatedRentalsNumber", $"uncompensatedReturnsNumber").otherwise($"uncompensatedRentalsNumber") / $"docksInService")
      //      .where($"nToDockRatio" > p / 100)
      .where($"uncompensatedReturnsNumber" =!= $"uncompensatedRentalsNumber")

    val anomaliesAsKeyValue: DataFrame = anomalies
      .select(
        to_json(struct(
          date_format($"start", "yyyy-MM-dd HH:mm").as("start"),
          date_format($"end", "yyyy-MM-dd HH:mm").as("stop"),
          $"arrivalsNumber",
          $"departuresNumber",
          $"name",
          $"uncompensatedReturnsNumber",
          $"uncompensatedRentalsNumber",
          $"docksInService",
          $"nToDockRatio"
        )).as("value"))
      .withColumn("key", hash($"value"))
      .select($"key".cast(StringType), $"value".cast(StringType))

    val queryAnomaly: StreamingQuery = anomaliesAsKeyValue
      .writeStream
      .outputMode(OutputMode.Update())
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "bicycles-anomaly-9")
      .option("checkpointLocation", "/tmp/bicycles-anomaly-9")
      .start()

    queryEtl.awaitTermination()
    queryAnomaly.awaitTermination()
  }
}
