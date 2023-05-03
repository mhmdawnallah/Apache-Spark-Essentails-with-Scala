package part2dataframes

import org.apache.spark.sql.functions.{avg, coalesce, col, countDistinct, lit, mean, stddev, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 1) It's often a good idea to do data aggregation and processing at the end of data processing to reduce data shuffling in case of wide transformation
 */
object AggregationsExercises extends App{
  var spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  testSumUpAllProfits(moviesDF)
  testDistinctMovieDirectorsCount(moviesDF)
  testShowMeanAndSTDVMovies(moviesDF)
  testAvgIMDBRatingGroupedByDirector(moviesDF)

  def testSumUpAllProfits(moviesDF: DataFrame): Unit = {
    moviesDF.select((
        col("US_Gross")
      + col("Worldwide_Gross")
      + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()
    assert(true)
  }

  def testDistinctMovieDirectorsCount(moviesDF: DataFrame): Unit = {
    moviesDF.select(countDistinct(col("Director")))
      .show()
  }

  def testShowMeanAndSTDVMovies(moviesDF: DataFrame): Unit = {
    moviesDF.agg(
      mean(col("US_Gross")),
      stddev(col("US_Gross"))
    ).show()
  }

  def testAvgIMDBRatingGroupedByDirector(moviesDF: DataFrame): Unit = {
    moviesDF.groupBy(col("Director"))
      .agg(
        avg(col("IMDB_Rating")).as("Avg_IMDB_Rating"),
        sum(col("US_Gross")).as("Total_US_Gross")
      )
      .orderBy(
        col("Avg_IMDB_RATING").desc_nulls_last
      )
      .show()
  }
}
