package part2dataframes

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.col

object JoinsExercises extends App {
  var spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagerDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  testMaxSalary(salariesDF, employeesDF)
  testNeverManagerEmployees(deptManagerDF, employeesDF)
  testJobTitles10BestEmployees(titlesDF, employeesDF)

  def testMaxSalary(salariesDF: DataFrame, employeesDF: DataFrame): Unit = {
    val maxSalariesPerEmpDF = salariesDF.groupBy("emp_no").max("salary")
    val employeesMaxSalariesDF = employeesDF.join(maxSalariesPerEmpDF, "emp_no")
    employeesMaxSalariesDF.show()
  }

  def testNeverManagerEmployees(deptManagerDF: DataFrame, employeesDF: DataFrame): Unit = {
    val neverManagersEmployeesDF = employeesDF.join(deptManagerDF, deptManagerDF.col("emp_no") === employeesDF.col("emp_no"),"left_anti")
    neverManagersEmployeesDF.show()
  }

  def testJobTitles10BestEmployees(titlesDF: DataFrame, employeesDF: DataFrame): Unit = {
    val mostRecentJobTitles = titlesDF.groupBy("emp_no", "title").agg(functions.max("to_date"))
    val maxSalariesPerEmpDF = salariesDF.groupBy("emp_no").agg(functions.max("salary").as("max_salary"))
    val employeesMaxSalariesDF = employeesDF.join(maxSalariesPerEmpDF, "emp_no").orderBy(col("max_salary").desc).limit(10)
    val bestPaidJobsDF = employeesMaxSalariesDF.join(mostRecentJobTitles, "emp_no")
    bestPaidJobsDF.show()
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
