package DFExercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

object DataframeExercise_2 extends App {
  /**
   * Exercises
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. create another column summing up the total profit of the movies = US_GROSS + WORLD
   * 3. select all comedy movies with IMDB rating above 6
   */

  val spark = SparkSession.builder()
    .appName("df exercise 2")
    .config("spark.master", "local")
    .getOrCreate()

  val movieDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  movieDf.show()

  // read the movies DF and select 2 columns

  import spark.implicits._

  val selectedDf = movieDf.select(
    col("Creative_Type"),
    $"Title")

  val selectedDfExpr = movieDf.selectExpr("Creative_Type", "Title")
  selectedDfExpr.show()

  // create another column summing up the total profit of the movies
  movieDf.select(
    $"Title",
    col("US_Gross"),
    'Worldwide_Gross,
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_profit")).show()

  movieDf.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_profit").show()

  // select all comedy movies with IMDB rating above 6

  movieDf.selectExpr("Major_Genre", "IMDB_Rating", "Title").filter("Major_Genre = 'Comedy' and IMDB_Rating > 6").show()
}
