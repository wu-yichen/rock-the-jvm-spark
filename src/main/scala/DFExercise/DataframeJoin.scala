package DFExercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataframeJoin extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master","local")
    .getOrCreate()

  val guitarsDf = spark.read.option("inferSchema","true").json("src/main/resources/data/guitars.json")
  val guitaristsDf = spark.read.option("inferSchema","true").json("src/main/resources/data/guitarPlayers.json")
  val bandDf = spark.read.option("inferSchema","true").json("src/main/resources/data/bands.json")

  // joins
  val joinCondition = guitaristsDf.col("band") === bandDf.col("id")

  // inner join
  val innerJoin = guitaristsDf.join(bandDf, joinCondition, "inner")
  innerJoin.drop(bandDf.col("id")).select("id","band")
  innerJoin.show()

  // left-outer join
  val leftJoin = guitaristsDf.join(bandDf, joinCondition, "left_outer")
  leftJoin.show()

  // right-outer join
  val rightJoin = guitaristsDf.join(bandDf, joinCondition, "right_outer")
  rightJoin.show()

  // outer join
  val outerJoin = guitaristsDf.join(bandDf, joinCondition, "outer")
  outerJoin.show()

  // left semi join
  val leftSemiJoin = guitaristsDf.join(bandDf, joinCondition, "left_semi")
  leftSemiJoin.show()

  // anti join
  val antiJoin = guitaristsDf.join(bandDf, joinCondition, "left_anti")
  antiJoin.show()

  // complex type
  guitaristsDf.join(guitarsDf.withColumnRenamed("id","guitarId"), expr("array_contains(guitars, guitarId)")) .show()

}
