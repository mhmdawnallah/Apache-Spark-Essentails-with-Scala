package part2dataframes

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, desc, mean, stddev}
import part2dataframes.Aggregations.moviesDF

/**
 * 1) Count with a single column exclude nulls but with * include nulls
 * 2) approx_count_distinct is a function in Apache Spark that estimates the number of distinct values in a given column or expression. It uses a statistical algorithm called "HyperLogLog" to approximate the distinct count with high accuracy and efficiency, even for very large datasets.
 * 2.1) The approx_count_distinct function is particularly useful for situations where the exact count of distinct values is not necessary, or where the dataset is too large to perform an exact count in a reasonable amount of time.
 * 2.2) This will return an estimated count of the distinct values in column_name of table_name. By default, Spark uses a relative error of 0.05 (i.e., the estimated count can be off by up to 5%), but you can adjust this by passing a second argument to the function
 */
object Aggregations extends App{
  var spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.tjson")

  testMajorGenresCount(moviesDF)
  testDistinctMajorGenresCount(moviesDF)
  testMajorGenresCount(moviesDF)
  testMajorGenreApproximateCount(moviesDF)
  testMinMaxIMDBRating(moviesDF)
  testSumUSGross(moviesDF)
  testMovieStatistics(moviesDF)
  testMajorGenreCountsGroupedBy(moviesDF)
  testMajorGenreAverageRatingGroupedBy(moviesDF)
  testAggregationByGenre(moviesDF)

  def testMajorGenresCount(moviesDF: DataFrame): Unit = {
    val genresCountDF = moviesDF.select(count(col("Major_Genre")))
    genresCountDF.show()
  }

  def testDistinctMajorGenresCount(moviesDF: DataFrame): Unit = {
    moviesDF.select(countDistinct(col("Major_Genre"))).show()
  }

  def testMajorGenreApproximateCount(moviesDF: DataFrame): Unit = {
    moviesDF.select(approx_count_distinct(col("Major_Genre"))).show
  }

  def testMajorGenreCountsGroupedBy(moviesDF: DataFrame): Unit = {
    val moviesByGenreCountsDF = moviesDF.groupBy("Major_Genre").count()
    moviesByGenreCountsDF.show()
    assert(true)
  }

  def testMajorGenreAverageRatingGroupedBy(moviesDF: DataFrame): Unit = {
    val moviesByGenreAvgRatingDF = moviesDF.groupBy("Major_Genre").avg("IMDB_RATING")
    moviesByGenreAvgRatingDF.show()
    assert(true)
  }

  def testAggregationByGenre(moviesDF: DataFrame): Unit = {
    val aggregatedMoviesByGenre = moviesDF.groupBy("Major_Genre")
      .agg(
        count("*").as("Number_of_Movies"),
        avg("IMDB_RATING").as("Avg_Rating")
      ).sort(desc("Avg_Rating"))
    aggregatedMoviesByGenre.show()
    assert(true)
  }

  def testMinMaxIMDBRating(moviesDF: DataFrame): Unit = {
    moviesDF.selectExpr("min(IMDB_RATING)").show()
    moviesDF.selectExpr("max(IMDB_RATING)").show()
    assert(true)
  }

  def testSumUSGross(moviesDF: DataFrame): Unit = {
    moviesDF.select(functions.sum(col("US_Gross")))
    moviesDF.selectExpr("sum(US_Gross)")
    assert(true)
  }

  def testMovieStatistics(moviesDF: DataFrame): Unit = {
    moviesDF.select(
      mean(col("Rotten_Tomatoes_Rating")),
      stddev(col("Rotten_Tomatoes_Rating"))
    ).show()
    assert(true)
  }





}
