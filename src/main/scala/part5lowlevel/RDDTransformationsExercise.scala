package part5lowlevel

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import part5lowlevel.RDDs.spark

import scala.io.Source
import scala.util.Try

object RDDTransformationsExercise extends App {
  case class Movie(title: String, genre: String, rating: Double)
  case class GenreAvgRating(genre: String, rating: Double)

  val spark = SparkSession.builder()
    .appName("RDD Transformations Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  val moviesRDD: RDD[Movie] =testReadMoviesRDD(sc)
  val moviesGenres: RDD[String] = testDistinctGenres(moviesRDD)
  val dramaMovies: RDD[Movie] = testDramaMoviesWithIMDBRatingGreaterThan6(moviesRDD)
  val averageRatingMoviesByGenre = testAverageRatingMoviesByGenre(moviesRDD)


  def testReadMoviesRDD(sc: SparkContext): RDD[Movie] = {
    val moviesList = readMovies("src/main/resources/data/movies.json")
    sc.parallelize(moviesList)
  }

  def testDistinctGenres(moviesRDD: RDD[Movie]): RDD[String] = {
    moviesRDD.map(_.genre).distinct()
  }

  def testDramaMoviesWithIMDBRatingGreaterThan6(moviesRDD: RDD[Movie]): RDD[Movie] = {
    moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)
  }

  def testAverageRatingMoviesByGenre(moviesRDD: RDD[Movie]): RDD[GenreAvgRating] = {
    moviesRDD.groupBy(_.genre).map{
      case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
    }
  }


  def readMovies(fileName: String): List[Movie] = {
    val source = Source.fromFile(fileName)
    try {
      source.getLines()
        .drop(1)
        .map(line => line.split(","))
        .map(tokens => Movie(tokens(0), tokens(9), tokens(13).toDouble))
        .toList
    } finally {
      Try(source.close())
    }
  }



}
