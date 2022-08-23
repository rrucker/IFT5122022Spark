/** DataFrameDatasetTutorial2020/2022> FlightJsonSparkGuideCh5  *** updated 09-23
 * * --- Spark is SCALA distributed over clusters ---
 * CH 5 of the Spark guide book, reading in JSON files, to create DFs
 * special scala syntax, a single back tick, allows a column to be specified
 * ***Deprecated for Scala 3***
 * NOTE: IN SCALA 3 THIS WILL BE DEPRECATED  ***
 * e.g.using the back tick:   'destination , also could be $"destination" , col("destination")
 * pretty standard, but check out the randomSplit pattern matched into an
 * array of DataFrames, at the end of this code base  :-)m( about line 98 )
 * ***** NEXT commentary is FROM THE DATABRICKS DOCS DESCRIPTION OF Datasets
 * "The Datasets API provides the benefits of RDDs (strong typing, ability to use powerful lambda
 * functions) with the benefits of Spark SQL’s optimized execution engine.
 * (** at the expense of requiring highly structured Data sets)
 * You can define Dataset JVM objects, and then manipulate them using functional transformations
 * (map, flatMap, filter, and so on) similar to an RDD. The benefits is that, unlike RDDs,
 * these transformations are now applied on a structured and strongly typed distributed
 * collection that allows Spark to leverage Spark SQL’s execution engine (Catalyst) for optimization."
 * **Keep in mind also the two big abstractions of Spark: DataFrame and Dataset are really ONE abstraction
 * Since a DataFrame IS  of type Dataset[ROW] . So a DataFrame = Dataset[Row]
 * 2020-09-20/21 rr  ( added databricks documentation 09-22 )
 *  ********** json local input file ********** on json object per line **
 * {"ORIGIN_COUNTRY_NAME":"Romania","DEST_COUNTRY_NAME":"United States","count":15}
 * {"ORIGIN_COUNTRY_NAME":"Croatia","DEST_COUNTRY_NAME":"United States","count":1}
 * {"ORIGIN_COUNTRY_NAME":"Ireland","DEST_COUNTRY_NAME":"United States","count":344}
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object FlightJsonSparkguideCh5{
def main(args: Array[String]):Unit = {   // standard program entry point
Logger.getLogger("org").setLevel(Level.OFF)
println(s"DataFrameDatasetlFlightData ${new java.util.Date()} ")// seamless integration with Java
type S= String; type I = Integer; type D = Double; type B = Boolean// type synonyms, less typing and errors
val spark = SparkSession // handle to the Spark ecosystem
.builder()
.appName("DataFrameDatasetFlight2020/21/22")
.master("local[*]")                     // use all available cores
.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")
import spark.implicits._                        // admit .toDF and .toDS methods
println(s" Spark version ${spark.version}")     //
val path3 = "/Users/rob/Desktop/2015-summary.json"   // my local json file from the Spark distro
println(s"Automatically Creating a JSON content DataFrame with  a SparkSession file read")
val dfRaw= spark.read.format("json")
              .load(path3)
// rename columns
val dfNewCols= dfRaw.withColumnRenamed("DEST_COUNTRY_NAME", "destination")
                      .withColumnRenamed("ORIGIN_COUNTRY_NAME", "origin")
println(s" Flight data with columns renamed")
dfNewCols.show(2, false)    // false means don't truncate rows

println(s" just as an example: Showing three columns selected using Scala syntax")
val dfScalaColumns =             // note : the symbol syntax is going away in Scala 3
   dfNewCols.select(  col("destination"),'destination, $"destination", expr("destination"))
dfScalaColumns.show(2)
println(s" Now go to the SQL API and create the Flight Table")
dfNewCols.createOrReplaceTempView("FlightTable")
println(s" use SQL API to select 'destination' from FlightTable")
// Scala & Spark use Immutable data ( Spark style)  and pure transformations ( pure functions)
// ** as compared to Python that uses mutable data:
  // e.g. set   a = 10 then later, a ="Bongo"  baaaaaaad
// The """ is Scala equivalent of a "heredoc" in python, the pipe '|'
  // and stripMargin will left-align text
spark.sql(""" Select destination As dest From FlightTable
  |Limit 3""".stripMargin)
println(s" Create a new column appended to the dfNewCols DF")
  println(s" The new column will equate to Booleans")
 val dfAppendedCol = dfNewCols.selectExpr("*", "destination = origin As withinCountry")
dfAppendedCol.show(3,false)
println(s" show the data types so far")
dfAppendedCol.printSchema()
println(s""" now I want to change the DF to a DS in order to achieve compile-time type safety
and, so I can do some special calculations, using the  case class features ( shown outside the object) """)
val ds = dfAppendedCol.as[caseAppended]
  // the case class converts each DF 'Row' objects to a case class
//instance of caseAppended. Now I have Dataset ( which IS a set of case class instances
println("s Now showing the Dataset, (a collection of case class instances")
// Dataset[caseAppended],, a DataFrame is of type Dataset[Row]
ds.show(5)
println(s" showing the Dataset as a list of native objects, ds.take(5)")
val result = ds.take(5)
result.foreach(println)
println(s"Now, just for grins, I want to change all the 'Booleans' in the appended col to 0/1")
// since I am in Dataset land, I can us all the methods such as lambdas and anonymous functions and
// higher order functions . A Lambda function is shown below
val dsCoded = ds.map{instance => if (instance.withinCountry) 1 else 0}
dsCoded.show(5)
println(s"getting random samples for exploratory data analysis EDA,( )maybe only want a fraction")
val seed = 42   // any fixed number allows replication of the random stream, for testing
val fraction = 0.02
val dsSample = ds.sample(true, fraction, seed)
println(s" sample with replacement (0.02)")
dsSample.show()
println(s" now do a randomSplit on the Dataset, pattern matched into a 2 component array")
val Array(d1,d2) = ds.randomSplit(Array(0.4,0.6), 42)
println(s" show the random Split dataframes , d1 and d2")
d1.show(5)
d2.show(3)


  }//end main()
}//end object
/** the case class caseAppended is used after a new column  is appended to the original
DataFrame
*/
case class caseAppended(destination: String, origin: String,count: Long, withinCountry: Boolean)

