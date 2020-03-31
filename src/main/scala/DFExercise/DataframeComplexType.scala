package DFExercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataframeComplexType extends App {
  val spark = SparkSession.builder()
    .appName("complex type")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // date
  val movieWithDateDf = moviesDf.select(
    col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy") as "Actual_Release")
    .where(not(col("Actual_Release").isNull))

  movieWithDateDf.show()

  movieWithDateDf.withColumn("Today", current_date())
    .withColumn("current_time", current_timestamp())
    .withColumn("Movie_age", datediff(col("Actual_Release"), col("Today")))
  //    .show()


  /**
   * Exercise
   * 1. How do we deal with multiple date formats
   * 2. Read the stocks Df and parse the dates
   */
  val stockDf = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stockDf.withColumn("New_Date", to_date(col("date"), "MMM d yyyy"))
    .show()

  // struct
  val newMovieDateDf = moviesDf.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")) as "Profit")
  //  newMovieDateDf.show()

  newMovieDateDf.withColumn("US_Profit", col("Profit").getField("US_Gross"))
    .show()

  // array
  val movieWithArrayDf = moviesDf.select(
    col("Title"),
    split(col("Title"), " ") as "Title_Words"
  )
  // movieWithArrayDf.show()

  movieWithArrayDf.select(
    col("Title"),
    size(col("Title_Words")),
    expr("Title_Words[0]"),
    array_contains(col("Title_Words"), "love")
  ) //.show()
}
