package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

trait RddLogging {

  def printCount[T](rdd:RDD[T], messagePrefix:String) = {
    rdd.cache()
    println(messagePrefix+rdd.count)
    rdd.unpersist()
  }

  def printRecords[T](recs:Array[T], dataStructure:String): Unit = {
    println(s" RECORDS of type:$dataStructure \n")
    if(recs.nonEmpty && recs.head.isInstanceOf[Row]) {
      val row = recs.head.asInstanceOf[Row]
      println("  Row schema:")

      Try{row.schema.fields.foreach(f => println(s"   ${Try{f.toString()}.getOrElse("null")}"))}.getOrElse(println("  no Schema"))
    }

    recs.foreach(record => println(s"  ${record.toString()}"))
  }



  def printDF(name:String, df:DataFrame) = {
    println("printing DF, START>>")
    println(s"$name Schema:\n")
    df.show()
    df.printSchema()
    /*    df.cache()
        val collected: Array[Row] = df.collect()
        printRecords(collected,s"$name DataFrame converted to RDD[Row]")*/
    println(s"printing DF, name: $name, END>>")
    //df.unpersist()
  }



  def printRddOfRows(name:String,rdd:RDD[Row])(implicit spark:SparkSession) = {

    rdd.cache()
    print(s"START>> check for errors rdd $name")
    printRecords(rdd.collect(),"Row")
    print(s"FINISHED>> checking $name \n")
    rdd.unpersist()
  }

  def printRdd[T](name:String,rdd:RDD[T],`type`:String)(implicit spark:SparkSession) = {
    rdd.cache()
    print(s"START>> check for errors rdd $name")
    printRecords(rdd.collect(),`type`)
    print(s"FINISHED>> checking $name \n")
    rdd.unpersist()
  }
}
