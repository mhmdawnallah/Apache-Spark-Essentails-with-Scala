package part4sql

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQL extends App {
  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  val tableNames = List(
    "departments",
    "dept_emp",
    "dept_manager",
    "employees",
    "salaries",
    "titles")

  testGetAmericanCarsDataFrameAPI(carsDF)
  testGetAmericanCarsSQL(carsDF)
  testTransferTablesFromPostgreSQLToSparkSQL(tableNames)
  testSQLPlayground()

  def testGetAmericanCarsDataFrameAPI(carsDF: DataFrame): Unit = {
    carsDF
      .select(col("Name")).where(col("Origin") === "USA")
      .show()
    assert(true)
  }

  def testGetAmericanCarsSQL(carsDF: DataFrame): Unit = {
    carsDF.createOrReplaceTempView("cars")
    val americanCars = spark.sql(
      """
        |SELECT name FROM cars WHERE origin = 'USA'
        |""".stripMargin
    )
    americanCars.show()

    assert(true)
  }

  def testSQLPlayground(): Unit = {
    spark.sql("Create database rtjvm")
    spark.sql("USE rtjvm")
    val databasesDF = spark.sql("SHOW databases")
    databasesDF.show()

    val employeesDF = spark.sql("SELECT * FROM employees")
    employeesDF.show()

    assert(true)
  }


  def testTransferTablesFromPostgreSQLToSparkSQL(tableNames: List[String]) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
//    tableDF.write
//      .mode(SaveMode.Overwrite)
//      .saveAsTable(tableName)
  }

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://192.168.1.16:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$tableName")
    .load()



}
