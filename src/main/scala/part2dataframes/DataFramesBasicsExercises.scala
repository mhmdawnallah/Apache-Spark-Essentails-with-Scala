package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
 * 1) Schema = List descriping the column names and types
 * 2) Each node in Spark Cluster receives the Schema (Column Names + Column Types) and a few rows
 * 3) DataFrame Partitioning
 * 4) DataFrames are Immutable
 * 5) Narrow transformations are transformations where each input partition contributes to only one output partition.
 *    This means that the data shuffling across the network is minimized, and the transformations are therefore more efficient.
 *    Examples of narrow transformations include filter, map, and union.
 * 6) Wide transformations are transformations where each input partition can contribute to multiple output partitions.
 *    This means that the data shuffling across the network is increased, and the transformations are therefore less efficient.
 *    Examples of wide transformations include groupByKey, reduceByKey, and join.
 * 7) Wide transformations can be more expensive in terms of computational resources and network bandwidth, as they require data shuffling across the network to ensure that all data with the same key ends up in the same partition.
 *    Therefore, minimizing the use of wide transformations in a Spark application is a good practice to ensure optimal performance.In general,
 *    it's recommended to use narrow transformations as much as possible and to only use wide transformations when necessary to achieve the desired result.
 * 8) Lazy Evaluation vs Eager Evaluation
 * 9) Spark Logical Plan: DataFrame Dependency Graph + Wide/Narrow Transformations sequence
 * 10) Spark Physical Plan: Optimized sequence of steps for nodes in the cluster
 * 11) Transformations how DataFrames are obtained and they are Lazy Evaluated (Lineage of Transformations)
 * 12) Actions are operations that trigger the computation and produce a result or write data to an external storage system.
 *     Examples of actions include collect, count, reduce, foreach, and save. When an action is called, Spark evaluates the lineage of transformations and executes the entire computation,
 *     including all the transformations, to produce the final result.
 *
 */
object DataFramesBasicsExercises extends App{
  var spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val smartphonesSchmea = StructType(Array(
    StructField("Make", StringType),
    StructField("model", StringType),
    StructField("screen dimension", DoubleType),
    StructField("camera megapixels", DoubleType),
    StructField("price", DoubleType)
  ))

  val smartphonesData = Seq(
    ("iPhone 12 Pro Max", "Apple", 6.7, 12, 1099.99),
    ("Galaxy S21 Ultra", "Samsung", 6.8, 108, 1199.99),
    ("Pixel 6 Pro", "Google", 6.7, 50, 899.99),
    ("OnePlus 9 Pro", "OnePlus", 6.7, 48, 899.99),
    ("Xperia 1 III", "Sony", 6.5, 12, 1299.99),
    ("Mi 11 Ultra", "Xiaomi", 6.81, 50, 1199.99)
  )
  val smartphonesDF = spark.createDataFrame(smartphonesData)
  smartphonesDF.show()
  smartphonesDF.printSchema()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  val moviesRowCounts = moviesDF.count()
  println(s"Movies Row Counts $moviesRowCounts")
  moviesDF.printSchema()


}
