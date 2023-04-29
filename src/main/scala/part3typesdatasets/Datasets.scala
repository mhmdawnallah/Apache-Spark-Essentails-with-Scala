package part3typesdatasets

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import part3typesdatasets.Datasets.numbersDF

/**
 * 1) Dataframe is a distributed collection of rows but Dataset is a distributed collection of JVM objects
 * 2) Serialization refers to the process of converting an object or data structure into a stream of bytes that can be easily transmitted over a network or stored to disk
 * 3) Deserialization is the reverse process of converting a stream of bytes back to an object or data structure.
 * 4) Encoders provide a way to perform this serialization and deserialization process efficiently. An Encoder specifies how to serialize and deserialize a specific type of JVM object to and from Spark's binary format. In Spark, we can define Encoders for custom types using the Encoders object. Encoders are particularly useful when working with Datasets, as they allow us to specify the data type of each column in the dataset, ensuring type safety and efficient serialization and deserialization.
 */
object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  testDatasetOfSimpleDataType(numbersDF)
  testDatasetOfComplexDataType(numbersDF)

  def testDatasetOfSimpleDataType(numbersDF: DataFrame): Unit = {
    implicit val intEncoder = Encoders.scalaInt
    val numbersDS: Dataset[Int] = numbersDF.as[Int]

    numbersDS.filter(_ < 100).show()
  }


  def testDatasetOfComplexDataType(numbersDF: DataFrame): Unit = {
    case class Car(
                  name: String,
                  milesPerGallon: Double,
                  cylinders: Long,
                  displacement: Double,
                  horsepower: Long,
                  weightInLbs: Long,
                  acceleration: Double,
                  year: String,
                  origin: String
                  )

    val carsDF = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/data/cars.json")

//    implicit val carEncoder = Encoders.product[Car](TagEncoders.product[Car])
//    val carsDS = carsDF.as[Car]
//    val carNamesUpperCaseDS: Dataset[String] = carsDS.map(car => car.name.toUpperCase())
//    carNamesUpperCaseDS.show()
  }


}
