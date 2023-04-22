package part2dataframes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

/**
 * 1) The technical term for selection is projection
 */
object ColumnsAndExpressions extends App{
  var spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  val firstColumn = carsDF.col("Name")
  val carNamesDF = carsDF.select(firstColumn)

  val carNamesDisplacements = carsDF.select(
    col("Name"),
    col("Displacement"),
    column("Year")
  )

  val carNamesDisplacementsShorter = carsDF.select("Name", "Year")

  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  testCarsWithKg3DF(carsDF)
  testCarsWithRenamedWeightInPounds(carsDF)
  testCarsFiltering(carsDF)
  testCarsUnion(carsDF)
  testCarsOriginDistinct(carsDF)

  def testCarsWithKg3DF(carsDF: DataFrame): Unit = {
    carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
    assert(true)
  }

  def testCarsWithRenamedWeightInPounds(carsDF: DataFrame): Unit = {
    carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
    assert(true)
  }

  def testCarsFiltering(carsDF: DataFrame): Unit = {
    val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
    val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
    val americanCarsDF = carsDF.filter("Origin = 'USA'")
    val americanPowerfulCarsChainedDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
    val americanPowerfulCarsChainedDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
    val americanPowerfulCarsChainedDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")
    assert(true)
  }

  def testCarsUnion(carsDF: DataFrame): Unit = {
    val moreCarsDF =spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
    val carsUnionedDF = carsDF.union(moreCarsDF)
    assert(true)
  }

  def testCarsOriginDistinct(carsDF: DataFrame): Unit = {
    val carOrigins = carsDF.select("Origin").distinct()
    carOrigins.show()
    assert(true)
  }

}
