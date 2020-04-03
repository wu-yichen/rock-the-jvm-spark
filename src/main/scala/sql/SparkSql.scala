package sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("spark sql")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")
  carDf.createOrReplaceTempView("cars")
  val americanCarsDf = spark.sql(
    """
      |select * from cars where Origin = 'USA'
      |""".stripMargin
  )
  //americanCarsDf.show()

  // we can run ANY SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databaseDf = spark.sql("show databases")
  //databaseDf.show()

  // transfer tables from a DB to Spark table
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transfer(tableNames: List[String], isWrite: Boolean = false) = tableNames.foreach {
    name =>
      val tableDf = readTable(name)
     // tableDf.createOrReplaceTempView(name)
      if (isWrite) tableDf.write.mode(SaveMode.Overwrite).saveAsTable(name)
  }

  transfer(List("departments", "dept_emp", "dept_manager", "employees", "salaries", "titles"), true)
  // read
  // val employeesDf = spark.read.table("employees")
  // employeesDf.show()

  /**
   * Exercise
   * 1. Read the movies DF and store it as a spark table in the rtjvm database
   * 2. count how many employees we have in between jan 1 2000 and jan 1 2001
   * 3. show the average salaries for the employees hired in between those dates grouped by department
   * 4. show the name of the best-paying department for employees hired in between those dates
   */
  // 1. Read the movies DF and store it as a spark table in the rtjvm database
  val movieDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  movieDf.write.mode(SaveMode.Overwrite).saveAsTable("movies")
  val movieTbl = spark.read.table("movies")
  //movieTbl.show()

  // 2. count how many employees we have in between jan 1 2000 and jan 1 2001
 spark.sql(
   """
     |select count(*) from employees where hire_date > '1999-01-01' and hire_date < '2001-01-01'
     |""".stripMargin)

  // 3. show the average salaries for the employees hired in between those dates grouped by department
  spark.sql(
    """
      |select avg(s.salary)  from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no = s.emp_no
      |group by de.dept_no
      |""".stripMargin
  )

  // 4. show the name of the best-paying department for employees hired in between those dates
  spark.sql(
    """
      |select dpt.dept_name , avg(s.salary) payments
      |from employees e, departments dpt, salaries s, dept_emp de
      |where e.emp_no = de.emp_no
      |and e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no = s.emp_no
      |and de.dept_no = dpt.dept_no
      |group by dpt.dept_name
      |order by payments desc
      |limit 1
      |""".stripMargin
  ).show()
}
