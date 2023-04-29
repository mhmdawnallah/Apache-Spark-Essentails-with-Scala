package part3typesdatasets

import java.sql.Date
import org.apache.spark.sql.functions.{col, mean, avg}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import part3typesdatasets.Datasets.numbersDF

/**
 * 1) Dataframe is a distributed collection of rows but Dataset is a distributed collection of JVM objects
 * 2) Serialization refers to the process of converting an object or data structure into a stream of bytes that can be easily transmitted over a network or stored to disk
 * 3) Deserialization is the reverse process of converting a stream of bytes back to an object or data structure.
 * 4) Encoders provide a way to perform this serialization and deserialization process efficiently. An Encoder specifies how to serialize and deserialize a specific type of JVM object to and from Spark's binary format. In Spark, we can define Encoders for custom types using the Encoders object. Encoders are particularly useful when working with Datasets, as they allow us to specify the data type of each column in the dataset, ensuring type safety and efficient serialization and deserialization.
 * 5) All case classes extend product types
 * 6) How to define dataest of a complex type:
 *  6.1) Define your case class
 *  6.2) Read the DF from the File
 *  6.3) Define an encoder (import the implicits)
 *  6.4) Convert the DataFrame to Dataset for type-safe processing
 * 7) Datasets can affect performance over DataFrame API in a positive or negative way, depending on the use case and how they are used.
 *  7.1) The Dataset API provides a more type-safe and object-oriented programming model compared to the DataFrame API, which is based on a more relational, SQL-like syntax. This makes Datasets more convenient and intuitive to use, especially for developers coming from an object-oriented programming background. Datasets also allow for compile-time type checking, which can help catch errors early in the development process.
 *  7.2) However, the additional type checking and object-oriented overhead in Datasets can also come with a performance cost. The overhead can cause additional memory consumption and CPU usage compared to the more streamlined DataFrame API. Additionally, the optimization techniques used by Spark's Catalyst optimizer, which underlies the DataFrame API, are more mature and comprehensive than those used by the Dataset API.
 *  7.3) In summary, whether Datasets are better or worse for performance compared to DataFrames depends on the use case and the specific implementation. For complex data processing tasks, it may be more beneficial to use DataFrames, while for more straightforward operations, Datasets may offer a more natural programming model and better type safety.
 */
object Datasets extends App {
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  val carsDF = readDF("cars.json")

  numbersDF.printSchema()

  testDatasetOfSimpleDataType(numbersDF)
  testDatasetOfComplexDataType(carsDF)
  testCarsCount(carsDF)
  testPowerfulCarsCount(carsDF)
  testGetAverageHorsePower(carsDF)

  def readDF(fileName: String): DataFrame = {
    spark.read
    .format("json")
    .option("inferSchema", "true")
    .load(s"src/main/resources/data/$fileName")
  }

  def testDatasetOfSimpleDataType(numbersDF: DataFrame): Unit = {
    import spark.implicits._
    val numbersDS: Dataset[Int] = numbersDF.as[Int]

    numbersDS.filter(_ < 100).show()
  }
  def testDatasetOfComplexDataType(carsDF: DataFrame): Unit = {
    import spark.implicits._
    val carsDS = carsDF.as[Car]
    val carNamesDS: Dataset[String] = carsDS.map(car => car.Name.toUpperCase())
    carNamesDS.show()
    assert(true)
  }

  def testCarsCount(carsDF: DataFrame): Unit = {
    println(s"Cars count is ${carsDF.count()}")
    assert(true)
  }

  def testPowerfulCarsCount(carsDF: DataFrame): Unit = {
    val powerfulCarsCount = carsDF.where(col("Horsepower") > 140).count()
    println(s"Powerful Cars count is ${powerfulCarsCount}")
    assert(true)
  }

  def testGetAverageHorsePower(carsDF: DataFrame): Unit = {
    carsDF.select(avg(col("Horsepower"))).show()
    assert(true)
  }

}
