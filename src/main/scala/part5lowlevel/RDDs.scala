package part5lowlevel

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source
import scala.util.Try

/**
 * 0) RDDS are distributed collections of JVM objects (storngly typed) like Dataset API
 * 1) Pros of Using RDD API over DataFrame and Dataset APIs:
 *  1.1) Fine-grained control: If you need fine-grained control over the computation, such as low-level transformations and actions, RDDs provide more flexibility than DataFrames and Datasets.
 *  1.2) First citizen of Spark: All high level APIs reduce to RDDs
 *  1.3) Partitioning can be controlled
 *  1.4) Order of elements can be controlled
 *  1.5) Order of operations matter for performance
 * 2) Cons of Using RDD API over DataFrame and Dataset APIs:
 *  2.1) You need to know the internals of Apache Spark for complex operations
 *  2.2) Poor APIs for quick data processing
 * 3) 99% of the time you're gonna use Dataframe/Dataset API unless in cases you care so much about performance
 * 4) Ways to create RDD:
 *  4.1) Parallelize the existing collection
 *  4.2) Read from a file
 *  4.3) Data Frame -> Data Set -> RDD
 * 5) You could obtain rdd easily from the dataset using rdd attribute
 */
object RDDs extends App {
  val spark = SparkSession.builder()
    .appName("Resilient Distributed Datasets (RDD) API")
    .config("spark.master", "local")
    .getOrCreate()
  val sc = spark.sparkContext


  val stocksDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/stocks.csv")

  case class StockValue(symbol: String, date: String, price: Double)

  testParallelizeCollection(sc)
  testRDDFromFile(sc, "src/main/resources/data/stocks.csv")
  testRDDFromFileEasier(sc, "src/main/resources/data/stocks.csv")

  testRDDFromDataFrame(stocksDF)
  testRDDConversionsToDatasetAndDataframe()

  def testParallelizeCollection(sc: SparkContext): Unit = {
    val numbers = 1 to 1000000
    val numbersRDD = sc.parallelize(numbers)
    println(numbersRDD.count())
    assert(true)
  }

  def testRDDFromFile(sc: SparkContext, fileName: String): Unit = {
    val stocksList: List[StockValue] = readStocks(fileName)
    val stocksRDD = sc.parallelize(stocksList)
    println(stocksRDD.count())
    assert(true)
  }

  def testRDDFromFileEasier(sc: SparkContext, fileName: String): Unit = {
    val dataRDD = sc.textFile(fileName)
      .map(line => line.split(","))
      .filter(tokens => tokens(0) == tokens(0).toUpperCase())
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
    println(dataRDD.count())
    assert(true)
  }

  def testRDDFromDataFrame(stocksDF: DataFrame): Unit = {
    import spark.implicits._
    val stocksDS = stocksDF.as[StockValue]
    val stocksRDD = stocksDS.rdd
    stocksRDD.count()
    assert(true)
  }

  def testRDDConversionsToDatasetAndDataframe(): Unit = {
    val numbers = 1 to 1000000
    val numbersRDD = sc.parallelize(numbers)
    import spark.implicits._
    val numbersDF = numbersRDD.toDF("numbers") // you lose the type safety here
    val numbersDS = spark.createDataset(numbersRDD) // you keep of the type safety
  }

  def readStocks(fileName: String): List[StockValue] = {
    val source = Source.fromFile(fileName)
    try {
      source.getLines()
        .drop(1)
        .map(line => line.split(","))
        .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
        .toList
    } finally {
      Try(source.close())
    }
  }



}
