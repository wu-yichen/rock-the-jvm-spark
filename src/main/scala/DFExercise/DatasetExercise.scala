package DFExercise

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object DatasetExercise extends App {
  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("data set")
    .getOrCreate()

  def getDataframe(fileName: String) =
    if (fileName.contains("csv"))
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv(s"src/main/resources/data/$fileName")
    else
      spark.read
        .option("inferSchema", "true")
        .json(s"src/main/resources/data/$fileName")


  val numberDf = getDataframe("numbers.csv")
  implicit val intEncoder = Encoders.scalaInt
  val numberDataset: Dataset[Int] = numberDf.as[Int]

  numberDataset.filter(_ < 100).show()

  // 1. case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  // 2. read car dataframe
  val carDf = getDataframe("cars.json")

  // 3. define an encoder

  import spark.implicits._

  val carDataset = carDf.as[Car]

  val carNameDs = carDataset.map(_.Name.toUpperCase)
  carNameDs.show()

  /**
   * Exercise
   * 1. count how many cars we have
   * 2. count how many powerful cars we have (HP>140)
   * 3. AVERAGE HP FOR THE ENTIRE DATASET
   */
  println(carDataset.count())
  println(carDataset.filter(car => car.Horsepower.getOrElse(0l) > 140).count())
  println(carDataset.map(_.Horsepower.getOrElse(0l)).reduce(_ + _) / carDataset.count())

  // joins
  /**
   * Exercise
   * join the guitarsDS and guitarPlayersDs, in an outer join
   */
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  val guitarDf = getDataframe("guitars.json")
  val guitarDs = guitarDf.as[Guitar]
  val guitarPlayerDf = getDataframe("guitarPlayers.json")
  val guitarPlayerDs = guitarPlayerDf.as[GuitarPlayer]

  val guitarAndGuitarPlayerDs = guitarDs.joinWith(
    guitarPlayerDs,
    array_contains(guitarPlayerDs.col("guitars"),guitarDs.col("id")), "outer")

  guitarAndGuitarPlayerDs.show()

  // grouping

  carDataset.groupByKey(_.Origin).count().show()
}
