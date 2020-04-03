package sql

import org.apache.spark.sql.SparkSession

object SparkSql extends App {
  val spark = SparkSession.builder()
    .appName("spark sql")
    .config("spark.master","local")
    .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
    .getOrCreate()

  val carDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")


  carDf.createOrReplaceTempView("cars")
  val americanCarsDf = spark.sql(
    """
      |select * from cars where Origin = 'USA'
      |""".stripMargin
  )
  americanCarsDf.show()

  // we can run ANY SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databaseDf = spark.sql("show databases")
  databaseDf.show()
}
