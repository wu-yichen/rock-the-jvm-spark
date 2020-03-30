package DFExercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataframeAggregationsExercise extends App {

  /**
   * Exercises
   * 1. Sum up all the profits of all the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */
  val spark = SparkSession.builder()
    .appName("aggregations exercise")
    .config("spark.master","local")
    .getOrCreate()

  val movieDf = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")

  // sum up all the profits of all the movies
  val profits = movieDf.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")) as "total_sales")
      .select(sum("total_sales"))
  profits.show()

  // count how many distinct directors we have
  //val directors = movieDf.select(approx_count_distinct(col("Director")))
  val directors = movieDf.select(countDistinct(col("Director")))
  directors.show()

  // show the mean and standard
  val data = movieDf.select(mean(col("US_Gross")), stddev(col("US_Gross")))
  data.show()

  // Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
  val avgDir = movieDf.groupBy(col("Director")).agg(
    avg(col("IMDB_Rating")) as "average_IMDB_Rating",
    avg(col("US_Gross")) as "average_US_Gross"
  ).orderBy(col("average_IMDB_Rating").desc_nulls_last)
  avgDir.show()
}
