package DFExercise

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataframeExercise extends App {
  /**
   * Exercise:
   * 1) Create a manual DF describing smartphones
   *  - make
   *  - model
   *  - screen dimension
   *  - camera megapixels
   * 2) Read another file from the data/folder, e.g. movies.json
   * - print its schema
   * - count the number of rows, call count()
   */
  val spark = SparkSession.builder()
    .appName("DataFrames Exercise")
    .config("spark.master", "local")
    .getOrCreate()


  val movieDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("mode", "failFast") // dropMalformed, permissive(default )
    .load("src/main/resources/data/movies.json")

  movieDf.printSchema()
  movieDf.show()
  println(s"there are ${movieDf.count()} rows")

  val mobile = Seq(
    ("Apple", "6s plus", "124", 245),
    ("Huawei", "mates", "154", 246),
    ("xiaomi", "mi3", "224", 345)
  )

  // create DF - option 1
  //val mobileDf = spark.createDataFrame(mobile)

  // option 2

  import spark.implicits._

  val mobileDfWithImplicit = mobile.toDF("make", "model", "screen dimension", "camera")

  mobileDfWithImplicit.show()
  mobileDfWithImplicit.printSchema()
  println(mobileDfWithImplicit.count())

  // write DF
  mobileDfWithImplicit.write
    .format("json") // parquet is the default format
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/mobile_dup")

  // use parquet can save space - it is binary compressed
  // default format is parquet
  movieDf.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movie_dup")

  // read DF
  val df = spark.read.options(Map(
    "inferSchema" -> "true",
    "mode" -> "failFast"
  )).json("src/main/resources/data/mobile_dup")

  df.show()

  // read text

  spark.read.text("src/main/resources/data/sampletxt.txt").show()
}
