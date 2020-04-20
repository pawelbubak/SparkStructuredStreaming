package com.bigdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._

object SparkApplication {
    def main(args: Array[String]) {
        val outputDirectory = "logasorcfiles"
        val checkpointDirectory = "checkpointdir"

        val spark = SparkSession.
            builder.
            appName("ChicagoDivvyBicycleSharingData").
            getOrCreate()

        val ds0 = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("project-data/stations/Divvy_Bicycle_Stations.csv").
            cache()

        ds0.show()

        import spark.implicits._
        val ds1 = spark.
            readStream.
            format("kafka").
            option("kafka.bootstrap.servers", "big-data-pb-w-0:9092").
            option("subscribe", "kafka-tt-bicycles-input").
            load()

        val tripsStream = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .as[(String, String)]

        val bicycleTripEventsDS: org.apache.spark.sql.Dataset[TripEvent] = tripsStream.map(_._2.split(","))
            .filter(_.length == 10)
            .map(array => TripEvent(array(0).toInt, array(1).toInt,
                array(2), array(3).toInt, array(4).toDouble,
                array(5), array(6), array(7).toInt,
                array(8).toDouble, array(9)))


        val bicycleTripsDS = bicycleTripEventsDS.select(
            to_timestamp($"datetime", "yyyy-MM-dd HH:mm:ss").as("ts"),
            $"event_type", $"trip_distance", $"passenger_count",
            $"tolls_amount", $"tip_amount", $"total_amount")

        val aggTripsDS = bicycleTripsDS.
            withWatermark("ts", "1 hour").
            groupBy(window($"ts", "1 hour", "1 hour"),$"event_type").
            agg(count("event_type").as("trips_count"),
                sum("passenger_count").as("passenger_sum"),
                sum("trip_distance").as("distance_sum"),
                sum("total_amount").as("amount_sum"))

        val query_test = aggTripsDS.
            writeStream.
            outputMode("update").
            trigger(ProcessingTime("1 minutes")).
            format("console").
            queryName("first-test").
            start()


    }
}
