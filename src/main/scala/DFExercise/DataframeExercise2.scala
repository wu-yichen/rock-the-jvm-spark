package DFExercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

// columns and expressions
object DataframeExercise2 extends App {
  val spark = SparkSession.builder()
    .appName("DF columns and expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")

  val firstCol = carsDF.col("name")

  // projecting

  import spark.implicits._

  println("--------------------projecting------------------------")
  val firstColDf = carsDF.select(
    firstCol,
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // scala symbol auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a column object
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg")) // EXPRESSION  selectExpr

  firstColDf.show()

  // DF processing

  // adding a column
  println("--------------------adding a column------------------------")
  carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2).show()

  // rename a column
  println("--------------------rename a column------------------------")
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
  carsWithColumnRenamed.show()


  // remove a column
  println("--------------------remove a column------------------------")
  carsWithColumnRenamed.drop("Cylinders", "Displacement").show()

  // filtering
  println("--------------------filtering------------------------")
  carsDF.filter(col("Origin") =!= "USA").show()
  carsDF.where(col("Origin") === "USA").show()

  // chain filters using expr
  println("--------------------chain filtering------------------------")
  carsDF.filter("Origin = 'USA' and Horsepower > 150").show()

  // unioning = adding more rows
  println("--------------------unioning------------------------")
  val moreCarsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)
  allCarsDF.show(100,false)

  carsDF.where("Name = 'ferrari enzo'").show()
  allCarsDF.filter("Name = 'ferrari enzo'").show()

  // distinct values
  println("--------------------distinct------------------------")
  carsDF.select("Origin").distinct().show()

}
