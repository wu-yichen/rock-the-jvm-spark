package DFExercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataframeAggregations extends App {

  val spark = SparkSession.builder()
    .appName("aggregations")
    .config("spark.master","local")
    .getOrCreate()

  val movieDf = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")

  val genresCountDf = movieDf.select(count(col("Major_Genre")))
  genresCountDf.show()

  //counting all
  val allGenresCountDf = movieDf.select(count("*"))
  allGenresCountDf.show()

  // counting distinct
  val distinctGenresCountDf = movieDf.select(countDistinct(col("Major_Genre")))
  distinctGenresCountDf.show()

  // counting approximate count
  val approximateGenresCountDf = movieDf.select(approx_count_distinct(col("Major_Genre")))
  approximateGenresCountDf.show()

  // min and max
  val minRatingDf = movieDf.select(min(col("IMDB_Rating")))
  minRatingDf.show()

  // sum
  val sumDf = movieDf.select(sum(col("US_Gross")))
  sumDf.show()

  // avg
  val avgDf = movieDf.select(avg(col("Rotten_Tomatoes_Rating")))
  avgDf.show()

  // data science
  movieDf.select(mean(col("Rotten_Tomatoes_Rating")), stddev(col("Rotten_Tomatoes_Rating"))).show()

  // grouping
  val groupDf = movieDf.groupBy(col("Major_Genre")).count()
  groupDf.show()

  // aggregations by genreDf
  val aggregationDf = movieDf.groupBy(col("Major_Genre")).agg(
    count("*") as  "N_Movies",
    avg(col("IMDB_Rating")) as "Avg_Rating"
  ).orderBy("Avg_Rating")
  aggregationDf.show()

}
