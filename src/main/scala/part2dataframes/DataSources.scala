package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

/**
 * 1) Reading a Data Frame requires:
 * 1.1) Format (Data Source Extension)
 * 1.2) Schema (optional)
 * 1.3) Zero or more options e.g: Mode Option (Fail Fast, Drop Malformed, Permissive)
 * 1.3.1) permissive — All fields are set to null and corrupted records are placed in a string column called _corrupt_record.
 * 1.3.2) dropMalformed — Drops all rows containing corrupt records.
 * 1.3.3) failFast — Fails when corrupt records are encountered.
 * 1.4) Data Source Path
 * 2) Writing a Data Frame requires:
 * 2.1) Format
 * 2.2) Save Mode (Overwrite, append, ignore, errorIfExists)
 * 2.3) Options (optional)
 * 2.4) Destination Path
 * 3) If Spark Failed Parsing the data as mentioned in the option it will put null instead (so be careful) because it may leads to misleads results
 */
object DataSources extends App{
  var spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast")
    .load("src/main/resources/data/cars.json")

  val sparkOptions = Map(
    "mode" -> "failFast",
    "path" -> "src/main/resources/data/cars.json",
    "inferSchema" -> "true"
  )
  val carsDFWithOptionsMap = spark.read
    .format("json")
    .options(sparkOptions)
    .load()

  carsDFWithOptionsMap.show()

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  // JSON Flags
  val carsDFWithDate = spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat","YYYY-MM-dd")
    .option("allowSingleQuotes", "true")
    .option("compression","uncompressed")
    .json("src/main/resources/data/cars.json")
  carsDFWithDate.printSchema()



}
