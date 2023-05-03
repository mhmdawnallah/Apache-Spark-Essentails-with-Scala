package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitarPlayersDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  testGuitarPlayersAndBandsJoinWithNoDuplicateColumns(guitarPlayersDF, bandsDF)
//  testGuitarPlayersAndBandsJoin(guitarPlayersDF, bandsDF)

  def testGuitarPlayersAndBandsJoin(guitarPlayers: DataFrame, bandsDF: DataFrame): Unit = {
    val guitarsBandsJoinCondition = guitarPlayers.col("band") === bandsDF.col("id")
    val guitarsBandsDF = guitarPlayers.join(bandsDF, guitarsBandsJoinCondition, "inner")
    guitarsBandsDF.show()

    val guitarsBandsLeftJoinDF = guitarPlayers.join(bandsDF, guitarsBandsJoinCondition, "left_outer")
    guitarsBandsLeftJoinDF.show()

    val guitarsBandsRightJoinDF = guitarPlayers.join(bandsDF, guitarsBandsJoinCondition, "right_outer")
    guitarsBandsRightJoinDF.show()

    val guitarBandsFullOuterJoinDF = guitarPlayers.join(bandsDF, guitarsBandsJoinCondition, "outer")
    guitarBandsFullOuterJoinDF.show()

    val guitarsBandsLeftSemiJoinDF = guitarPlayers.join(bandsDF, guitarsBandsJoinCondition, "left_semi")
    guitarsBandsLeftSemiJoinDF.show()

    val guitarsBandsLeftAntiJoinDF = guitarPlayers.join(bandsDF, guitarsBandsJoinCondition, "left_anti")
    guitarsBandsLeftAntiJoinDF.show()

    val guitarsBandsRightAntiJoinDF = guitarPlayers.join(bandsDF, guitarsBandsJoinCondition, "right_anti")
    guitarsBandsRightAntiJoinDF.show()

    assert(true)
  }

  def testGuitarPlayersAndBandsJoinWithNoDuplicateColumns(guitarPlayers: DataFrame, bandsDF: DataFrame): Unit = {
    val guitarsBandsDF = guitarPlayers.join(bandsDF.withColumnRenamed("id", "band"), "band")
    guitarsBandsDF.show()

    assert(true)
  }
}
