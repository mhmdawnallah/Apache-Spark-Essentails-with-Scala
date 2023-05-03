package part3typesdatasets

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object ManagingNulls extends App {
  var spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  testCoalesce(moviesDF)
  testMoviesWhereRottenTomatoRatingIsNull(moviesDF)
  testMoviesRottenTomatoRatingOrderedNullsFirst(moviesDF)
  testRemovingNulls(moviesDF)
  testReplacingNullsWithZeros(moviesDF)
  testComplexExpressions(moviesDF)

  def testCoalesce(moviesDF: DataFrame): Unit = {
    moviesDF
      .select(
        col("Title"),
        col("Rotten_Tomatoes_Rating"),
        col("IMDB_Rating"),
        coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
      )
      .show()
    assert(true)
  }

  def testMoviesWhereRottenTomatoRatingIsNull(moviesDF: DataFrame): Unit = {
    moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull).show()
    assert(true)
  }

  def testMoviesRottenTomatoRatingOrderedNullsFirst(moviesDF: DataFrame): Unit = {
    moviesDF.orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_first).show()
    assert(true)
  }

  def testRemovingNulls(moviesDF: DataFrame): Unit = {
    moviesDF.select("Title", "Rotten_Tomatoes_Rating", "IMDB_Rating").na.drop().show()
    assert(true)
  }

  def testReplacingNullsWithZeros(moviesDF: DataFrame): Unit = {
    moviesDF.na.fill(0, List("Rotten_Tomatoes_Rating", "IMDB_Rating")).show()
    moviesDF.na.fill(Map(
      "IMDB_Rating" -> 0,
      "Rotten_Tomatoes_Rating" -> 10,
      "Director" -> "Unknown"
    )).show()

    assert(true)
  }

  def testComplexExpressions(moviesDF: DataFrame): Unit = {
    moviesDF.selectExpr(
      "Title",
      "IMDB_Rating",
      "Rotten_Tomatoes_Rating",
      "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull",
    ).show()
    assert(true)
  }

}
