package DFExercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataframeNull extends App {
  val spark = SparkSession.builder()
    .appName("manage nulls")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDf = spark.read.option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  // select the first non null value
  moviesDf.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"),col("IMDB_Rating")) as "final_rating"
  ).show()

  // checking for null
  moviesDf.select("Title","Rotten_Tomatoes_Rating").where(col("Rotten_Tomatoes_Rating").isNotNull).show()

  // nulls when ordering
  moviesDf.select("Title","Rotten_Tomatoes_Rating").orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_last).show()

  // removing nulls
  moviesDf.select(col("Title"),col("Rotten_Tomatoes_Rating")).na.drop()
      .show()

  // replace nulls
  moviesDf.na.fill(Map(
    "Rotten_Tomatoes_Rating" -> 0,
    "IMDB_Rating" -> 10
  )).select( "Rotten_Tomatoes_Rating" ,"IMDB_Rating" ).show()

  // complex operations
  moviesDf.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating,IMDB_Rating) as ifnull",   // same as coalesce
    "nvl(Rotten_Tomatoes_Rating,IMDB_Rating) as nvl", //same
    "nvl2(Rotten_Tomatoes_Rating,IMDB_Rating * 10, 0.0) as nvl2", // if (first != null) second else third
    "nullif(Rotten_Tomatoes_Rating,IMDB_Rating) as nullif" // if (first == second) null else first
  ).show()

}
