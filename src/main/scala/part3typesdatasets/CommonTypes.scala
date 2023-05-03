package part3typesdatasets

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, corr, initcap, lit, regexp_extract}

object CommonTypes extends App {
  var spark = SparkSession.builder()
    .appName("Common Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  testAddPlainValueToColumn(moviesDF)
  testCorrelationBetweenTomatoAndIMDBRating(moviesDF)
  testStringsSparkDataTypeTransformation(carsDF)
  testRegexTransformation(carsDF)

  def testAddPlainValueToColumn(moviesDF: DataFrame): Unit = {
    moviesDF.select(col("Title"), lit(46).as("plain_value")).show()
    val goodMovieCondition = col("IMDB_Rating") > 8.0
    val goodRatingMovies = moviesDF.select(col("Title"), goodMovieCondition.as("is_good_movie"))
    goodRatingMovies.show()
    goodRatingMovies.where("is_good_movie").show()
    assert(true)
  }

  def testCorrelationBetweenTomatoAndIMDBRating(moviesDF: DataFrame): Unit = {
    val correlation = moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")
    println(correlation)
    assert(correlation < 0.5, "Rotten Tomatoes Rating and IMDB Rating are strongly positively correlated")
  }

  def testStringsSparkDataTypeTransformation(carsDF: DataFrame): Unit = {
    carsDF.select(initcap(col("Name")).as("Name Capitalized")).show()
  }

  def testRegexTransformation(carsDF: DataFrame): Unit = {
    val regexString = "volkswagen|vw"
    carsDF.select(
      col("Name"),
      regexp_extract(col("Name"), regexString, 0).as("regexp_extract")
    )
    .where(col("regexp_extract") =!= "")
    .drop("regex_extract")
    .show()
  }





}
