package part2dataframes

import org.apache.spark.sql.SparkSession

/**
 * 1) Narrow Transformation
 * 2) Wide Transformation -> Shuffling (data exchange between cluster nodes)
 */
object NarrowVsWideTransformations extends App{
  var spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val rdd = spark.sparkContext.parallelize(1 to 100)
  val rdd2 = rdd.map(x => x + 1)
  val rdd3 = rdd.filter(x => x % 2 == 0)
  val rdd4 = rdd.map(x => (x % 2, x)).reduceByKey(_ + _)
  println("rdd2: " + rdd2.collect().mkString(", "))
  println("rdd3: " + rdd3.collect().mkString(", "))
  println("rdd4: " + rdd4.collect().mkString(", "))

}
