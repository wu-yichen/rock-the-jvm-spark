package DFExercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataframeCommonType extends App {
  val spark = SparkSession.builder()
    .appName("common type")
    .config("spark.master","local")
    .getOrCreate()
  val movieDf = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")

  val goodMovie = col("Major_Genre") === "Drama" and col("IMDB_Rating") > 7.0

  val goodMovieDf = movieDf.select(col("title"), goodMovie as "good_movie")
  goodMovieDf.show()

  val notGoodMovieDf = goodMovieDf.where(not(col("good_movie")))
  notGoodMovieDf.show()

  // string

  val carDf = spark.read.option("inferSchema","true").json("src/main/resources/data/cars.json")
  val regexString = "volkswagen|vw"
  val vwCarDf = carDf.select(
    col("name"),
    regexp_extract(col("name"), regexString, 0) as "regex_extract"
  ).where(col("regex_extract") =!= "")
  vwCarDf.show()


  /**
   * Exercise
   * Filter the cars DF by a list of car names obtained by the getCarNames function
   */
  def getCarNames: List[String] = List("Volkswagen","Mercedes-Benz","Ford")
  val regexStr = getCarNames.map(_.toLowerCase()).mkString("|")
  val filteredCarDf = carDf.select(
    col("name"),
    regexp_extract(col("name"),regexStr,0) as "regex"
  ).where(col("regex") =!= "").drop("regex")
  filteredCarDf.show()
}
