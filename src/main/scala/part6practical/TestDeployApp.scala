package part6practical

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object TestDeployApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Need the input and output path arguments")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Test Deploy Spark Application to the Cluster")
      .getOrCreate()

    val moviesDF = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load(args(0))

    val goodComedyMoviesDF = moviesDF.select(
      col("Title"),
      col("Major_Genre"),
      col("IMDB_Rating")
      ).where(col("Major_Genre" ) === "Comedy" && col("IMDB_Rating") > 6.5)

    goodComedyMoviesDF.show()

    goodComedyMoviesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }

}
