/** file is ASG31.txt (in advanced AI) 10 pairs of values
 * and do linear regression
 * 2022-08-30rr
 * // ASG31.txt file contents
 * first col = X (predictor), second col = Y ( label)
 * See companion program just using Spark
 * thee parametrs  are th4e same
 * 10,2
5,4
1,4
6,2
7,4
3,5
4,4
5,5
1,6
8,4
 */
import scala.math._
import scala.io.Source
type D = Double; type V = Vector[D]
val fileName = "/Users/rob/Desktop/ASG31.txt"
 val lines =  scala.io.Source.fromFile(fileName).getLines()
val pairlist= for (line <- lines.toList)
  yield{
  val Array(x,y) = line.split(",")
    (x.toDouble,y.toDouble)
  }
val h:(List[D],List[D]) = pairlist.unzip
val X = h._1.toVector   // raw X values
val Y = h._2.toVector   // raw Y values

def mean(v:V):D = v.sum/v.size
def center( v:V) =v.map( _ - mean(v))   // standardize a vector
def dot(v:V, w: V):D  = (v zip w).map{case(x,y) => x * y }.sum
def norm(v: V):D = math.sqrt(dot(v,v))
//  center the vectors first before correlation calcs
//NOTE: this coefficient IS the cosine of the angle
//between two standardized vectors
def correlationCoefficient(v:V, w:V):D =
  dot(center(v),center(w))/(norm(center(v)) * norm(center(w)))
val test = List(1,2,3).map(_.toDouble).toVector
// center the variables
val x = center(X)
val y = center(Y)
val regression = dot(x,y)/dot(x,x)  // this is "b", regress coef
val theta = correlationCoefficient(x:V , y:V)* 180.0/math.Pi
val intercept = mean(Y) - regression * mean(X)
// Y = 5.45 - 0.29 * X    regression line
//NOTE the variance of a vector is proportionnal
// to its squared length, i.e its self dot product
val SST = dot(y,y)
val bx = x.map( _ * regression)
val SSR= dot(bx,bx)
val error = (y.zip(bx)).map{case(s,t)=> s-t} // residuals
val sumerror = error.sum
val SSE = dot(error,error)
















//for{line <- lines} yield line

  /*
  lines.map{ line =>
   val Array(x,y) = line.split(",")
   (x,y)
 }*/


    //val Array(x,y) = line.split(",")
    //println(s"x")
    //(x.toDouble,y.toDouble)




/*
val filename = "fileopen.scala"
for (line <- Source.fromFile(filename).getLines) {
    println(line)
}
 */
/*
def scalaFiles =
    for {
        file <- filesHere
        if file.getName.endsWith(".scala")
    } yield file
 */


/*
val fileName = "data.txt"
for (lines <- scala.io.Source.fromFile(fileName).getLines()) {
  // do something with lines
}
 */

//lines.unzip[Int,Int]

}// end A32SparkLinRegWk30906
case class Data(features: org.apache.spark.ml.linalg.Vector, label: Double)

