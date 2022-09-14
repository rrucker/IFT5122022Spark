/**IFT512FallAssignments.Quiz1Fall
 *All code is within the SPARK environment
 * PART I the scala section
 * ***** SCALA section within your SPARK program
 * 1. Manually enter the X,Y vectors ( as scala vectors
 X is the predictor and Y is the label
 * Note the type V =
 *        scala.collection.immutable.Vector
 * is synonym to do this withib since SPARK
 * also has its own  ml Vector type
 * X 3.0, 4,5)  Y = 6.0, 9.0, 15
 * using functions define3d WITHIN SPARK ( see
 * examples in code snippets below)
 * compute:
 * SST, SSR, SSE, MSE,RMSE, b (regression coeff)
 *   r, r2
 * Draw the statistical triangle showing the
 * centered y vector and the b* x vector and the
 * error vector plus theta
 *
 * PART II the SPARK section
 * *************  SPARK  section of the code *****
 * "x", the centered X vector is a vector of Doubles
 * so, convert each x component into a ml vector
 * i.e. Vectors.dense(component)
 * ( a SPARK requirement for linear regression)
 *  then pair up this vector with the "y" label
 *  to create a DF
 *  Now compute the standard linear regression
 *  val lr = new LinearRegression()
 *  . . .
 *
 *
 *
 *
 *
 * 2022-09-10 rr
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.math._
import org.apache.spark.sql._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

println(s"Spark Linreg ${new java.util.Date()}")
type S = String;type D = Double
type V = scala.collection.immutable.Vector[D]
val spark = SparkSession
  .builder()
  .appName("BigDAssignmentsTutorials")
  .master("local[*]")
  .getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")
import spark.implicits._
spark.version
// The data source!
val X = Vector(3.0,4.0,5.0)
val Y = Vector (6.0,9.0,15.0)
def center(v: V):V = v.map(_ - v.sum/v.size)
def dot(v:V,w:V):D =
  (v zip w).map{case(x,y) => x*y}.sum
def multiply(x: D, v: V)= v.map(_ * x)
def subtract(v:V, w:V):V=
  (v zip w).map{case(x,y)=> x-y}
def norm(v: V):D = math.sqrt(dot(v,v))
val x = center(X)
val y = center(Y)
//regression coeff
val b = dot(x,y)/dot(x,x)
val SST = dot(y,y)
val temp = multiply(b,x)
val SSR = dot (temp,temp)
// error vector = y - bx
val temp2 = subtract(y, temp)
val SSE = dot(temp2,temp2)
val r = dot(x,y)/(norm(x) * norm(y))
val r2 = r * r
/* ******************************************** */
/*   Now The SPARK approach ************  */
// Now convert scala vector
// components to ml "Vectors"
val mlVectors =
   x . map(component => Vectors.dense(component))
val training = (mlVectors zip y).toDF("features","label")
training.show()
val lr = new LinearRegression()
// Fit the model
val lrModel = lr.fit(training)

// Print the coefficients and intercept for linear regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

// Summarize the model over the training set and print out some metrics
val trainingSummary = lrModel.summary
println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"r2: ${trainingSummary.r2}")