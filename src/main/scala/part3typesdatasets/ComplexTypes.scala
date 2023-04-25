package part3typesdatasets

import org.apache.spark.sql.functions.{array_contains, col, expr, size, split, struct, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

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
  testStructsWithColOperators(moviesDF)
  testStructsWithStringExpressions(moviesDF)
  testTitleSplittedArrays(moviesDF)

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

  def testStructsWithColOperators(moviesDF: DataFrame): Unit = {
    moviesDF
      .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
      .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
      .show()

    assert(true)
  }

  def testStructsWithStringExpressions(moviesDF: DataFrame): Unit = {
    moviesDF
      .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
      .selectExpr("Title", "Profit.US_Gross")
      .show()

    assert(true)
  }

  def testTitleSplittedArrays(moviesDF: DataFrame): Unit = {
    val moviesWithTitleWordsSplittedDF = moviesDF
      .select(col("Title"), split(col("Title"), " |,").as("Title_Words"))

    moviesWithTitleWordsSplittedDF
      .select(
        col("Title"),
        expr("Title_Words[0]"),
        size(col("Title_Words")),
        array_contains(col("Title_Words"), "Love")
      )
      .show()

    assert(true)
  }
}
