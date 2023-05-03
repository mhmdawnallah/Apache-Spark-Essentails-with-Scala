package part7bigdata

import org.apache.spark.sql.functions.{avg, col, count, from_unixtime, hour, lit, max, mean, min, round, stddev, sum, to_date, unix_timestamp}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object TaxiApplication extends App {
  val spark = SparkSession.builder()
    .appName("Taxi Big Data Applications")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._
  val taxiesDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiesDF.printSchema()
  println(s"Taxy Records count is ${taxiesDF.count()}")

//  val taxiesBigDF = spark.read.load("/home/ubuntu/Downloads/NYC_taxi_2009-2016.parquet")
//  println(taxiesBigDF.count())

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  val rateCodeIDs = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/RateCodeIds.csv")

  taxiZonesDF.printSchema()
  println(s"Taxi Zones Records count is ${taxiZonesDF.count()}")

  /** Questions to Answer on Taxis Big Dataset
   * 1) Which zones have the most pickup/dropoffs overall ?
   * 2) What are the peak hours for taxi ?
   * 3) How are the trips distributed by length? Why are people taking the cab ?
   * 4) What are the peak hours for long/short trips ?
   * 5) What are the top 3 pickup/dropoff zones for long/short trips ?
   * 6) How are people paying for the ride on long/short trips ?
   * 7) How is the payment type evolving with time ?
   * 8) Can we explore a ride-sharing opportunity by grouping close short trips ?
  */

  // Question 1
  val pickupTaxiZonesDF = taxiesDF.join(taxiZonesDF, taxiesDF.col("PULocationID") === taxiZonesDF.col("LocationID"), "inner")
      .drop("LocationID", "service_zone")
      .groupBy("PULocationId", "Zone", "Borough")
      .agg(count("*").as("total_pickup_trips"))
      .orderBy(col("total_pickup_trips").desc_nulls_last)
  pickupTaxiZonesDF.show()

  // Question 1
  val dropOffTaxiZonesDF = taxiesDF.join(taxiZonesDF, taxiesDF.col("DOLocationID") === taxiZonesDF.col("LocationID"), "inner")
    .drop("LocationID", "service_zone")
    .groupBy("DOLocationID", "Zone", "Borough")
    .agg(count("*").as("total_dropoff_trips"))
    .orderBy(col("total_dropoff_trips").desc_nulls_last)
  dropOffTaxiZonesDF.show()

  // Question 1
  val pickupsByBoroughDF = pickupTaxiZonesDF.groupBy("Borough")
    .agg(sum("total_pickup_trips").as("total_pickup_trips"))
    .orderBy(col("total_pickup_trips").desc_nulls_last)
  pickupsByBoroughDF.show()

  // Question 1
  val dropOffByBoroughDF = dropOffTaxiZonesDF.groupBy("Borough")
    .agg(sum("total_dropoff_trips").as("total_dropoff_trips"))
    .orderBy(col("total_dropoff_trips").desc_nulls_last)
  dropOffByBoroughDF.show()

  // Question 2
  val pickupByHoursDF = taxiesDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)
  pickupByHoursDF.show()

  // Question 3
  val tripDistanceDF = taxiesDF.select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30
  val tripDistanceStatsDF = tripDistanceDF.select(
    count("*").as("count"),
    lit(longDistanceThreshold).as("threshold"),
    mean("distance").as("mean_distance"),
    stddev("distance").as("stddev_distance"),
    min("distance").as("min_distance"),
    max("distance").as("max_distance")
  )
  tripDistanceStatsDF.show()

  val tripsWithLengthDF = taxiesDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()
  tripsByLengthDF.show()

  // Question 4
  val pickupByHoursLongTripsDF = tripsWithLengthDF
    .filter(col("isLong") === true)
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)
  pickupByHoursLongTripsDF.show()

  // Question 4
  val pickupByHoursShortTripsDF = tripsWithLengthDF
    .filter(col("isLong") === false)
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)
  pickupByHoursShortTripsDF.show()

  // Question 5
  val longTripsPopularityPickup = pickupPopularityZones(col("isLong") === true, 3)
  val shortTripsPopularityPickup = pickupPopularityZones(col("isLong") === false, 3)
  longTripsPopularityPickup.show()
  shortTripsPopularityPickup.show()

  // Question 6
  val longTripsPaymentMethodsDF = paymentMethodsPopularity(col("isLong") === true)
  val shortTripsPaymentMethodsDF = paymentMethodsPopularity(col("isLong") === false)
  longTripsPaymentMethodsDF.show()
  shortTripsPaymentMethodsDF.show()

  taxiesDF.where(col("passenger_count") < 3).select(count("*")).show()
  taxiesDF.select(count("*")).show()

  // Question 7
  val rateCodeEvolutoon = taxiesDF
    .groupBy(to_date(col("pickup_datetime")).as("pickup_day"), col("rate_code_id"))
    .agg(count("*").as("total_trips"))
    .orderBy(col("pickup_day"), col("total_trips").desc_nulls_last)
  rateCodeEvolutoon.show()


  def pickupPopularityZones(longPredicate: Column, limit: Int): DataFrame = {
    tripsWithLengthDF.filter(longPredicate)
      .join(taxiZonesDF, taxiesDF.col("PULocationID") === taxiZonesDF.col("LocationID"), "inner")
      .drop("LocationID", "service_zone")
      .groupBy("Zone")
      .agg(count("*").as("total_pickup_trips"))
      .orderBy(col("total_pickup_trips").desc_nulls_last)
      .limit(limit)
  }

  def paymentMethodsPopularity(longPredicate: Column): DataFrame = {
    tripsWithLengthDF.filter(longPredicate)
      .join(rateCodeIDs, "RatecodeID")
      .groupBy("RatecodeID")
      .agg(count("*").as("total_trips"))
      .orderBy(col("total_trips").desc_nulls_last)
  }

}
