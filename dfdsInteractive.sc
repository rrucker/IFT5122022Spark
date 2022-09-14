
/** DataFrameDatasetTutorial2020/2022 > dfdsInteractive.sc
Going from a Scala sequence of ( nr, height, weight)
-> DataFrame-> Dataset -> then extract the height , weight
 as Vectors, then compute the correlation coefficient
 between these vectors
 (in-class  programming
2021-01-21/2022-08rr
 Points to note:SPARK is a scala domain specific language(DSL)
( that changed the World!!!)
Main SPARK data structures = Dataframes(DF), Datasets(DS), SQL tables
 1. Dataframes(DF) are the first choice for (optimized)processing
 2. SQL api is available as alternatice access to Dataframes
 3. Datasets are TYPED Dataframes and allow Java/Scala code direct
 ( in fact a Dataframe IS a Dataset of type ROW, ie
 Dataframe = Dataset[Row]
However, SPARK keeps the DataFrame internals mostly hidden.
Note our guide text says range is a Dataframe ( nope, its a Dataset)
 4. The scala connection shows that a Spark Dataset IS a set of
 case class instances!!
 *******   Scala notes *****
 well, all the code here is scala! so every line shows scala
 a. check out higher order functions that take functions as arguments
 b. I write a small set o stat functions to illustrate scala,
 along the way I calc the correlation coefficient for a couple of vectors
btw: the correlation coef IS just the cosine between two centered vecs
 and their linear regression parameters
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import scala.math._

Logger.getLogger("org").setLevel(Level.OFF)

val spark = SparkSession
.builder()
.appName("dfdsInteractive.sc")
.master("local[*]")
.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")
import spark.implicits._
spark.version
type S = String;type D = Double; type I = Integer
type V = Vector[D]
val myRangeX = spark.range(8)
myRangeX.as[Long]       // convert to Long Dataset
myRangeX.collect()      // bring all data baxk to driver ( CRASH is possible)
val fil = myRangeX.map( x => x * 2)  //myRngeX.map(_ * 2)
fil.collect()
val myRange = spark.range(8).toDF("Nr")
myRange.count()
/** case class to convert DF --> DS */
case class  Data(age: I, height: I)
val dataValues = Seq(
 (12, 60),
 (14, 62),
 (15, 62),
 (18, 70),
 (17, 69)
).toDF("age", "height")
dataValues.show()
print(s" show a collect() on a Dataframe versus a Dataset")
dataValues.collect()
//  now convert to a DS
val ds = dataValues.as[Data]
ds.show()
ds.first()
ds.take(2)  // note an array of case instances
ds.select($"age")
//ds.col("age")
val xraw = ds.collect().map{ d => d.age}
val yraw = ds.collect().map{ d => d.height}
//println(s" X = $xraw ")

/*  A Tiny  stats set of functions
illustrating scala high3er order functioms:
E.G> to find the correlation coefficient between
two centered vectors & the regression line
need some stats  functions
1. set up needed functions:
Let v, w be centered Double vectors
2. then dot(v,w) = |v| * |w| Cos ( theta)
 theta is angle between them , since they are
 centered,then
 cos(theta) is the correlation coefficient)
 and arccos * 180/Pi is degrees between them
*/
type V = Vector[D]
/* dot product ( inner product) */
def dot(v:V, w:V):D =
(v zip w).map{case (x,y)=>x*y}.sum
def norm(v:V):D = math.sqrt(dot(v,v))
def mean(v:V):D= v.sum/v.size
def center(v:V):V = v.map{_ - mean(v)}
def center1 (v:V):V = v.map{x => x - mean(v)}
/*I want to use the Data case class to convert a
DF to a DS so I can use user (Java/Scala)functions on it
*/

case class Data(nr: I, height:D, weight:D)
/* some simple values of height to weight */
val clientsDF = Seq(
(1, 60.0, 120.0),
(2, 65.0, 130.0),
(3, 72.0, 169.0),
(4, 70.0, 150.0 ),
(5, 73.0, 140.0)
).toDF("nr", "height", "weight")
clientsDF.show()
  /* convert to dataset */
val clientsDS = clientsDF.as[Data]
clientsDS.show()
val X = clientsDS.collect().map{c =>c.height}.toVector
val Y = clientsDS.collect().map{c => c.weight}.toVector
val x1 = center(X)
val y1 = center(Y)
// dot ( a,b) = |a| |b| cos( angle between)
 val cosine= dot(x1,y1)/(norm(x1)* norm(y1))
val arccosine = acos(cosine) * 180.0/math.Pi
val regressionCoefficient = dot(x1,y1)/dot(x1,x1)
val  b = regressionCoefficient
val intercept =  mean(Y) - b * mean(X)


/*  end stats example ************  */

println(s" show the various  column methods ")
val c1 = col("c1")
val r1 = spark.range(3)
r1.show()
r1.printSchema()
r1.col("id")
r1.select(col("id"))
//r1.select('id)
r1.select($"id")
r1.select(expr("id"))
println(s"show return types of collect() in DataFrames & Datasets")
// r1 is a dataset

//r1.toDF
r1.toDF("id")
r1.toDF("id").show()
r1.toDF("id").collect()
r1.toDF("id").limit(2).show()


val mySchema = StructType(Seq(
StructField("room", StringType),
StructField("length", IntegerType),
StructField("area", DoubleType)
))

// short and easy way to construct a DF 
case class Data2(room: S,length: I ,area:D)
val seq1 = Seq(
("A", 10, 100.0),
("B", 20, 400.0),
("C", 30, 900.0))
val df1 = seq1.toDF("room","length","area")
df1.take(3)
df1.collect()

val ds2 = df1.as[Data2]
ds2.take(3)
ds2.collect()
ds2.limit(2)
ds2.limit(2).show()
val (xc, yc)=  ds2.collect()
             .map{ r=> (r.room, r.length)}.unzip

val (xt, yt)=  ds2.take(3)
.map{ r=> (r.room, r.length)}.unzip
 ds2.limit(3)
 ds2.limit(3).select("room")


//Bar( Seq("A","B","C")), (3,6,10), name="Bar").plot(title= "Doodles")

val rdd1 = spark.sparkContext.makeRDD(seq1)
//val rowedRdd1 =  //resilient distributed dataset//val dfrdd = spark.createDataFrame(rdd1, mySchema)
//dfrdd.count()
//dfrdd.show(3)
val test = rdd1.map{case(r, l, a ) => Row(r, l.toInt, a.toDouble)}
val testDF = spark.createDataFrame(test,mySchema)
testDF.show()
val triples = Vector((1,2,3), (3,4,5),(5,6,7))
val dfp= triples.toDF("a","b","c")
dfp.show()
val sums = triples.map{case(a,b,c)=> a + b + c}

val mySchema2 = StructType(Seq(
   StructField("x", IntegerType),
   StructField("y", StringType),
   StructField("z", DoubleType)
))

val seq2 = Seq((1, "a", 3.4),(2, "b", 5.8))
seq2.toDF("x","y","z").printSchema()
val rdd2 = spark.sparkContext.makeRDD(seq2)
val rowedRdd2 =  // resilient distributed dataset
// parallelize(seq1)
   rdd2.map{case(x,y,z ) => Row(x.toInt, y.toString, z.toDouble)}
val rdd2DF = spark.createDataFrame(rowedRdd2,mySchema2)
//val data1 = ???
// create a rdd first then a DS
//val rdd = spark.sparkContext.makeRDD(seq1)
//rdd.foreach(println)

//val df3 = spark.createDataFrame(rdd, mySchema)



/*
case class MyClass(val1: String, valN: Long = 0L)
val df = rdd.map({
  case Row(val1: String, valN: Long) => MyClass(val1, valN)
}).toDF("col1_name", "colN_name")

*/
