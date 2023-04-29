package part3typesdatasets

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.array_contains
import part3typesdatasets.DatasetsJoins.guitarPlayersDS

/**
 * 1) Joins and Groups are considered Wide Transformations (Shuffles) and could change the number of partitions
 */
object DatasetsJoins extends App {

  case class Guitar(
                     id: Long,
                     model: String,
                     make: String,
                     guitarType: String
                   )

  case class GuitarPlayer(
                           id: Long,
                           name: String,
                           guitars: Seq[Long],
                           band: Long
                         )

  case class Band(
                   id: Long,
                   name: String,
                   hometown: String,
                   year: Long
                 )

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

  import spark.implicits._
  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]
  val carsDS = readDF("cars.json").as[Car]

  testJoiningWithDatasets(guitarsDS, guitarPlayersDS, bandsDS)
  testGroupingCarsByOriginDataset(carsDS)


  def readDF(fileName: String): DataFrame = {
    spark.read
      .format("json")
      .option("inferSchema", "true")
      .load(s"src/main/resources/data/$fileName")
  }

  def testJoiningWithDatasets(guitarsDS: Dataset[Guitar], guitarPlayersDS: Dataset[GuitarPlayer], bandsDS: Dataset[Band]): Unit = {
    val guitarPlayersBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
    guitarPlayersBandsDS.show()

    val guitersGuitarPlayersDS: Dataset[(Guitar, GuitarPlayer)] = guitarsDS.joinWith(guitarPlayersDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    guitersGuitarPlayersDS.show()

    assert(true)
  }

  def testGroupingCarsByOriginDataset(carsDS: Dataset[Car]): Unit = {
    val carsGroupedByOriginCount = carsDS
      .groupByKey(_.Origin)
      .count()
      .show()

    assert(true)
  }
}
