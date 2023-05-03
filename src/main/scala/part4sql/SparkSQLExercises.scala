package part4sql

import java.sql.Date
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import part4sql.SparkSQL.{spark, tableNames}
import part4sql.SparkSQLExercises.moviesDF

object SparkSQLExercises extends App {
  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  spark.sql("Create database rtjvm")
  spark.sql("USE rtjvm")

  val tableNames = List("employees", "departments", "dept_emp", "salaries")
  testTransferTablesFromPostgreSQLToSparkSQL(tableNames)


  testCreateMoviesSparkTable(moviesDF)
  testEmployeesCountBetweenTwoDates()
  testEmployeesAverageSalariesGroupedByDepartment()

  def testCreateMoviesSparkTable(moviesDF: DataFrame): Unit = {
    moviesDF.createOrReplaceTempView("movies")
    assert(true)
  }

  def testEmployeesCountBetweenTwoDates(): Unit = {
    spark.sql(
      """
        |SELECT hire_date FROM employees
        |""".stripMargin).show()
    val employeesHiredCount = spark.sql(
      s"""
        |SELECT COUNT(*) AS employees_count
        |FROM employees
        |WHERE hire_date > '1999-01-01' AND hire_date < '2001-01-01'
        |""".stripMargin)
    employeesHiredCount.show()

    assert(true)
  }

  def testEmployeesAverageSalariesGroupedByDepartment(): Unit = {
    spark.sql(
      """
         |SELECT dept_name, AVG(salaries.salary) AS average_employees_salary
         |FROM employees
         |INNER JOIN dept_emp
         |ON dept_emp.emp_no = employees.emp_no
         |INNER JOIN departments
         |ON departments.dept_no = dept_emp.dept_no
         |INNER JOIN salaries
         |ON salaries.emp_no = employees.emp_no
         |WHERE hire_date > '1999-01-01' AND hire_date < '2001-01-01'
         |GROUP BY dept_name
         |ORDER BY average_employees_salary DESC
         |""".stripMargin
    ).show()

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
