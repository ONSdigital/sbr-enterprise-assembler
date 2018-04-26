package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

trait RddLogging {

  def printRecords[T](recs:Array[T], dataStructure:String): Unit = {
    println(s" RECORDS of type:$dataStructure \n")
    if(recs.head.isInstanceOf[Row]) {
      val row = recs.head.asInstanceOf[Row]
      println("  Row schema:")

      Try{row.schema.fields.foreach(f => println(s"   ${Try{f.toString()}.getOrElse("null")}"))}.getOrElse(Unit)
    }

    recs.foreach(record => println(s"  ${record.toString()}"))
  }

  def printDF(name:String, df:DataFrame) = {
    println("printing DF, START>>")
    println(s"$name Schema:\n")
    df.printSchema()
    df.cache()
    val collected: Array[Row] = df.collect()
    printRecords(collected,s"$name DataFrame converted to RDD[Row]")
    println("printing DF, END>>")
    df.unpersist()
  }

  def printRdd[T](name:String,rdd:RDD[T],`type`:String)(implicit spark:SparkSession) = {
    rdd.cache()
    print(s"START>> check for errors rdd $name")
    printRecords(rdd.collect(),`type`)
    print(s"FINISHED>> checking $name \n")
    rdd.unpersist()
  }
}