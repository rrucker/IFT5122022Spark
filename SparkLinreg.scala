

/** Tutorial on reading in a csv file for use in
 * linear regression.
 * I read the text file, split each line into x, y
 * then convert the x values to dense vector and insert
 * into a case class ( this is now a Dataset!
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.math._
import org.apache.spark.sql._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.LinearRegression
/** IFT512FallAssignments. SparkLinreg
 this is the companion to the A3_2SparkScala program  that
works thru the assignment A3.2 using just scala and hand coded functs
 This exercise works thru the same exercise using just Spark tools
2020-09-05/2022-08-31
rr Machine Learning Engineering track
 */
object SparkLinreg extends App{
  Logger.getLogger("org").setLevel(Level.OFF)
  println(s"Spark Linreg ${new java.util.Date()}")
  type S= String; type D = Double; type V = Vector
  // see companion program A3_2SparkScala for file detail
  val path = "/Users/rob/Desktop/ASG31.txt"
  val spark = SparkSession
    .builder()
    .appName("BigDAssignmentsTutorials")
    .master("local[*]")
    .getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  import spark.implicits._
  println(s"Spark version = ${spark.version}")
  // read file: this results in 10 lines of string  "10,2" "5,4"   ,
  // then I have to get the 10 and the 2 via "split"
  val dataInitial= spark.read.textFile(path)
  //just a check on the first line
  println(s"first line of file , ${dataInitial.first()}")
  // now split each string line, and insert into a case class
  // that matches
  // what the ml algorithm needs
  //namely, a "features" column and a "label" column
  // --- this is a Dataset, strongly typed for production
  // Note: I didn't use "vector assembler,
  // just the manual conversion to dense vector.
  // the output here is a Dataset( = instances of a case class)
  // note the Data case class has the proper column names
  val ds = dataInitial.map{ line => {
    val Array(x,y) = line.split(",")
    val vecx = Vectors.dense(x.toDouble)
    Data(vecx, y.toDouble)
  }
  }
  ds.show()
  println(s" spark.version ${spark.version}")
  val lr = new LinearRegression()

  // Fit the model
  val lrModel = lr.fit(ds)

  // Print the coefficients and intercept for linear regression
  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  // Summarize the model over the training set and print out some metrics
  val trainingSummary = lrModel.summary
  println(s"numIterations: ${trainingSummary.totalIterations}")
  println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
  trainingSummary.residuals.show(false)
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")

}// end
case class Data(features: org.apache.spark.ml.linalg.Vector, label: Double)





