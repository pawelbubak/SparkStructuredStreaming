package com.bigdata.spark.model

case class Station(
                    id: Int,
                    name: String,
                    totalDocks: Int,
                    docksInService: Int,
                    status: String,
                    latitude: Double,
                    longitude: Double,
                    location: String
                  )
