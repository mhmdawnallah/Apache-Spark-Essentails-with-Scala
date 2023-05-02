package part7bigdata

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, from_unixtime, round, sum, to_date, unix_timestamp}
import part7bigdata.TaxiApplication.taxiZonesDF

object TaxiEconomicImpact {
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Need 1) Big data source, 2) Taxi Zones data sourec, 3) Output data destination")
      System.exit(1)
    }
    /**
     * Args are:
     * 1) Big Data Source
     * 2) Taxi Zones
     * 3) Output data source
     */
    val spark = SparkSession.builder()
      .appName("Taxi Big Data Applications")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    val taxiesBigDF = spark.read.load(args(0))
    val taxiZonesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))

    taxiesBigDF.printSchema()

    val percentGroupAttempt = 0.05
    val percentAcceptGrouping = 0.3
    val discountGroup = 5
    val extraCostIndividual = 2
    val avgCostReduction = 0.6 * taxiesBigDF.select(avg(col("total_amount"))).as[Double].take(1)(0)
    val percentGroupable = 289623 * 1.0 / 331893

    // We get all the trips that happen in the same location within the 5 minutes time window frame
    val groupedAttemptsRideSharingDF = taxiesBigDF
      .select(round(unix_timestamp(col("pickup_datetime")) / 300).cast("integer").as("fiveMinId"), col("pickup_taxizone_id"), col("total_amount"))
      .groupBy(col("fiveMinId"), col("pickup_taxizone_id"))
      .agg((count("*") * percentGroupable).as("total_trips"), sum("total_amount").as("total_amount"))
      .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
      .drop("fiveMinId")
      .join(taxiZonesDF, col("pickup_taxizone_id") === col("LocationID"), "inner")
      .drop("LocationID", "service_zone")
      .orderBy(col("total_trips").desc_nulls_last)
    groupedAttemptsRideSharingDF.show()

    val groupingEstimateEconomicImpactDF = groupedAttemptsRideSharingDF
      .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
      .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discountGroup))
      .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCostIndividual)
      .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))
    groupingEstimateEconomicImpactDF.show()

    // On the dataset with day only: 39987$/day = 14,395320 Million per year Estimated
    // On the dataset from 2009 to 2016: 139million dollar profit for the taxi company hola :)
    val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum("totalImpact").as("total_impact"))
    totalProfitDF.show()
    totalProfitDF.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(args(2))
  }
}