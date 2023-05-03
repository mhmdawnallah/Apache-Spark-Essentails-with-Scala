package part3typesdatasets

import org.apache.spark.sql.functions.{col, lit, lower, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CommonTypesExercise extends App{
  var spark = SparkSession.builder()
    .appName("Common Types Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  testFilterCars(carsDF, getCarNames)

  def testFilterCars(carsDF: DataFrame, carNames: List[String]): Unit = {
    val complexRegex = carNames.map(_.toLowerCase).mkString("|")
    carsDF.select(
      col("Name"),
      regexp_extract(col("Name"), complexRegex, 0).as("regexp_extract")
    ).where(col("regexp_extract") =!= "").drop("regexp_extract").show()

    val carNameFilters = carNames.map(_.toLowerCase()).map(name => col("name").contains(name))
    val bigFilterCarNames = carNameFilters.fold(lit(false))((accFilter, newCarNameFilter) => accFilter or newCarNameFilter)
    carsDF.filter(bigFilterCarNames).show()
  }

  def getCarNames: List[String] = {
    // API Call
    List("ford maverick", "Datsun pl510", "None")
  }
}
