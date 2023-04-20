package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

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
