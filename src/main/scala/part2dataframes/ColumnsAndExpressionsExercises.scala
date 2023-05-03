package part2dataframes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit}

object ColumnsAndExpressionsExercises extends App{
  var spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .format("json")
    .load("src/main/resources/data/movies.json")

  testMoviesTitleWithReleaseDateDF(moviesDF)
  testMoviesTotalProfitDF(moviesDF)
  testComedyMoviesWithIMDBAbove6(moviesDF)

  def testMoviesTitleWithReleaseDateDF(moviesDF: DataFrame): Unit = {
    val moviesTitleWithReleaseDateDF = moviesDF.select("Title", "Release_Date")
    moviesTitleWithReleaseDateDF.show()
    assert(true)
  }

  def testMoviesTotalProfitDF(moviesDF: DataFrame): Unit = {
    val moviesWithTotalProfit = moviesDF.withColumn(
      "Total_Profit",
        coalesce(col("US_GROSS"), lit(0))
      + coalesce(col("Worldwide_Gross"), lit(0))
      + coalesce(col("US_DVD_Sales"), lit(0))
    )
    val moviesTitleWithTotalProfit = moviesWithTotalProfit.select("Title", "Total_Profit")
    moviesTitleWithTotalProfit.show()
    assert(true)
  }

  def testComedyMoviesWithIMDBAbove6(moviesDF: DataFrame): Unit = {
    val comedyMoviesWithIMDBAbove6DF = moviesDF
      .select("Title", "Major_Genre", "IMDB_Rating")
      .filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
    comedyMoviesWithIMDBAbove6DF.show()
    assert(true)
  }

}
