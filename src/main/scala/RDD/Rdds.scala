package RDD

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object Rdds extends App {

  val spark = SparkSession.builder()
    .appName("spark RDD")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  val num = 1 to 1000
  val numRdd = sc.parallelize(num)

  // 1 - read from file
  case class StockValue(symbol: String, date: String, price: Double)

  def readFromFile(fileName: String) = {
    Source.fromFile(fileName).getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(token => StockValue(token(0), token(1), token(2).toDouble))
      .toList
  }

  val stockRDD = sc.parallelize(readFromFile("src/main/resources/data/stocks.csv"))

  // 1-b read from files
  val stockRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(token => token(0).toUpperCase == token(0))
    .map(token => StockValue(token(0), token(1), token(2).toDouble))

  //1-c read from df
  val stockDf = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stockRDD4 = stockDf.rdd

  import spark.implicits._

  val stockDs = stockDf.as[StockValue]
  val stockRDD3 = stockDs.rdd

  // RDD to DF
  val stockDf1 = stockRDD3.toDF("symbol", "date", "price") //  you lose the type info

  // RDD to DS
  val stockDs1 = spark.createDataset(stockRDD3)


  // transformation

  // distinct
  val msftRDD = stockRDD3.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count()

  val companyNamesRDD = stockRDD3.map(_.symbol).distinct()

  // min and max
  implicit val stockOrdering = Ordering.fromLessThan((sa: StockValue, sb: StockValue) =>
    sa.price < sb.price)
  val minMsft = msftRDD.min()

  // reduce
  numRdd.reduce(_ + _)

  // grouping
  val groupedStockRDD = stockRDD3.groupBy(_.symbol) // very expensive

  // partitioning - is expensive. Involves shuffling best practice: partition early then process that.
  // size of a partition 10 - 100 MB
  val repartitionedStockRDD = stockRDD3.repartition(30)
  repartitionedStockRDD.toDF()
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks-30.csv")

  // coalesce does not involve shuffling
  val coalescedRDD = repartitionedStockRDD.coalesce(15)
  coalescedRDD.toDF()
    .write.mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks-15.csv")

  /**
   * Exercise
   * 1. Read the movies.json as an RDD
   * 2. show the distinct genres as an RDD
   * 3. select all the movies in the Drama genre with IMDB rating > 6
   * 4. show the average rating of movies by genre.
   */
  case class Movie(title: String, genre: String, rating: Double)

  val movieDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val movieRDD = movieDf.select(
    col("Title") as ("title"),
    col("Major_Genre") as ("genre"),
    col("IMDB_Rating") as ("rating")
  ).where(col("genre").isNotNull && col("rating").isNotNull).as[Movie].rdd

  // 2. show the distinct genres as an RDD
  val genreMovieRDD = movieRDD.map(_.genre).distinct()

  // 3. select all the movies in the Drama genre with IMDB rating > 6
  val goodMovieRDD = movieRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  // 4. show the average rating of movies by genre.
  movieRDD.toDF().groupBy(col("genre")).avg("rating").show()

  movieRDD.toDF().show()
  genreMovieRDD.toDF().show()
  goodMovieRDD.toDF().show()
}
