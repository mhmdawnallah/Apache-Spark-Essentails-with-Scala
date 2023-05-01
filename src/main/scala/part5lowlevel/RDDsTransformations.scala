package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import part5lowlevel.RDDsTransformations.stocksDF

/**
 * 1) Distinct is a lazy transformation
 * 2) Repartitioning is an expensive operation because it does shuffling
 * 3) Best practice is to repartition early and then do data processing
 * 4) Best practice for partition size is from 10-100 MigaByte not too small or not too big for efificent data processing and avoid bottlenecks on the processing nodes
 * 5) Best practice for partitions count is 2-4 partition for CPU to maximize the parallelism and avoid underutilization of resources
 */

object RDDsTransformations extends App {
  case class StockValue(symbol: String, date: String, price: Double)

  val spark = SparkSession.builder()
    .appName("RDD Transformations")
    .config("spark.master", "local")
    .getOrCreate()

  val stocksDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/stocks.csv")

  val stocksRDD = convertStocksDFToRDD(stocksDF)
  println(stocksRDD.count())

  val microsoftStocksRDD = testFilterMicrosoftStocks(stocksRDD)
  println(s"Microsoft Stocks count is ${microsoftStocksRDD.count()}")

  val stockSymbols: RDD[String] = testFilterStockSymbols(stocksRDD)
  stockSymbols.foreach(symbol => println(symbol))

  testRDDPartitioning(stocksRDD)

  def convertStocksDFToRDD(stocksDF: DataFrame): RDD[StockValue] = {
    import spark.implicits._
    val stocksDS = stocksDF.as[StockValue]
    stocksDS.rdd
  }

  def testFilterMicrosoftStocks(stocksRDD: RDD[StockValue]): RDD[StockValue] = {
    import spark.implicits._
    stocksRDD.filter(_.symbol == "MSFT")
  }

  def testFilterStockSymbols(stocksRDD: RDD[StockValue]): RDD[String] = {
    stocksRDD.map(_.symbol).distinct()
  }

  def testRDDPartitioning(stocksRDD: RDD[StockValue]): Unit = {
    import spark.implicits._
    val repartitionedStocksRDD = stocksRDD.repartition(30)
    repartitionedStocksRDD.toDF.write
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/data/stocks30")

    val coalescedRDD = repartitionedStocksRDD.coalesce(15)
    coalescedRDD.toDF.write
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/data/stocks15")

  }

}
