package part3typesdatasets

import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ComplexTypesExercise extends App{
  var spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    .getOrCreate()

  val stocksDF = spark.read
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  testConvertDateStringToDateType(stocksDF)

  def testConvertDateStringToDateType(stocksDF: DataFrame): Unit = {
    val stocksConvertedDateDF = stocksDF.withColumn("date_converted", to_date(col("date"),"MMM d yyyy"))
    stocksConvertedDateDF.show()
  }


}
