package part7bigdata

import org.apache.spark.sql.SparkSession

object TaxiApplication extends App {
  val spark = SparkSession.builder()
    .appName("Taxi Big Data Applications")
    .config("spark.master", "local")
    .getOrCreate()

  val taxiesDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiesDF.printSchema()
  println(taxiesDF.count())

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  taxiZonesDF.printSchema()
  println(taxiZonesDF.count())
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




}
