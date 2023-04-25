package part3typesdatasets

import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ComplexTypes extends App {
  var spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  testConvertDateStringToDateType(moviesDF)
  testMisleadingInterpretedDateResults(moviesDF)

  def testConvertDateStringToDateType(moviesDF: DataFrame): Unit = {
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    moviesDF.select(col("Title"), to_date(col("Release_Date"),"dd-MMM-yy").as("Actual_Release")).show()
    assert(true)
  }

  def testMisleadingInterpretedDateResults(moviesDF: DataFrame): Unit = {
    val moviesWithReleaseDates = moviesDF.select(col("Title"), to_date(col("Release_Date"),"dd-MMM-yy").as("Actual_Release"))
    moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull).show()
    assert(true)
  }
}
