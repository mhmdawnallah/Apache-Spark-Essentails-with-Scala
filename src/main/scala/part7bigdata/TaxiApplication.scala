package part7bigdata

import org.apache.spark.sql.functions.{col, count, hour, lit, max, mean, min, stddev, sum}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object TaxiApplication extends App {
  val spark = SparkSession.builder()
    .appName("Taxi Big Data Applications")
    .config("spark.master", "local")
    .getOrCreate()

  val taxiesDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiesDF.printSchema()
  println(s"Taxy Records count is ${taxiesDF.count()}")

//  val taxiesBigDF = spark.read.load("/home/ubuntu/Downloads/NYC_taxi_2009-2016.parquet")
//  println(taxiesBigDF.count())

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  taxiZonesDF.printSchema()
  println(s"Taxi Zones Records count is ${taxiZonesDF.count()}")

  /** Questions to Answer on Taxis Big Dataset
   * 1) Which zones have the most pickup/dropoffs overall ?
   * 2) What are the peak hours for taxi ?
   * 3) How are the trips distributed ? Why are people taking the cab ?
   * 4) What are the peak hours for long/short trips ?
   * 5) What are the top 3 pickup/dropoff zones for long/short trips ?
   * 6) How are people paying for the ride on long/short trips ?
   * 7) How is the payment type evolving with time ?
   * 8) Can we explore a ride-sharing opprtunity by grouping close short trips ?
  */

  val pickupTaxiZonesDF = taxiesDF.join(taxiZonesDF, taxiesDF.col("PULocationID") === taxiZonesDF.col("LocationID"), "inner")
      .drop("LocationID", "service_zone")
      .groupBy("PULocationId", "Zone", "Borough")
      .agg(count("*").as("total_pickup_trips"))
      .orderBy(col("total_pickup_trips").desc_nulls_last)
  pickupTaxiZonesDF.show()

  val dropOffTaxiZonesDF = taxiesDF.join(taxiZonesDF, taxiesDF.col("DOLocationID") === taxiZonesDF.col("LocationID"), "inner")
    .drop("LocationID", "service_zone")
    .groupBy("DOLocationID", "Zone", "Borough")
    .agg(count("*").as("total_dropoff_trips"))
    .orderBy(col("total_dropoff_trips").desc_nulls_last)
  dropOffTaxiZonesDF.show()

  val pickupsByBoroughDF = pickupTaxiZonesDF.groupBy("Borough")
    .agg(sum("total_pickup_trips").as("total_pickup_trips"))
    .orderBy(col("total_pickup_trips").desc_nulls_last)
  pickupsByBoroughDF.show()

  val dropOffByBoroughDF = dropOffTaxiZonesDF.groupBy("Borough")
    .agg(sum("total_dropoff_trips").as("total_dropoff_trips"))
    .orderBy(col("total_dropoff_trips").desc_nulls_last)
  dropOffByBoroughDF.show()

  val pickupByHoursDF = taxiesDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)
  pickupByHoursDF.show()

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

  val pickupByHoursLongTripsDF = tripsWithLengthDF
    .filter(col("isLong") === true)
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)
  pickupByHoursLongTripsDF.show()

  val pickupByHoursShortTripsDF = tripsWithLengthDF
    .filter(col("isLong") === false)
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)
  pickupByHoursShortTripsDF.show()
  val longTripsPopularityPickup = pickupPopularityZones(col("isLong") === true, 3)
  val shortTripsPopularityPickup = pickupPopularityZones(col("isLong") === false, 3)
  longTripsPopularityPickup.show()
  shortTripsPopularityPickup.show()

  def pickupPopularityZones(longPredicate: Column, limit: Int): DataFrame = {
    tripsWithLengthDF.filter(col("isLong") === true)
      .join(taxiZonesDF, taxiesDF.col("PULocationID") === taxiZonesDF.col("LocationID"), "inner")
      .drop("LocationID", "service_zone")
      .groupBy("Zone")
      .agg(count("*").as("total_pickup_trips"))
      .orderBy(col("total_pickup_trips").desc_nulls_last)
      .limit(limit)
  }

}
