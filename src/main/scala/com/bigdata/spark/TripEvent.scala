package com.bigdata.spark

case class TripEvent(
                        id: Int,
                        eventType: Int, // rozpoczęcie 0, zakończenie 1
                        eventTime: String,
                        stationId: Int,
                        duration: Double,
                        userType: String,
                        gender: String,
                        week: Int,
                        temperature: Double,
                        events: String
                    )
