package closures

import dao.hbase.HFileUtils
import global.{AppParams, Configs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import spark.RddLogging
import spark.extensions.sql._

import scala.util.Try

/**
  *No calculations added
  */
class NewPeriodClosure extends HFileUtils with BaseClosure with RddLogging with Serializable {

  def addNewPeriodData(appconf: AppParams)(implicit spark: SparkSession,con:Connection): Unit = {
    val allLUsDF: DataFrame = getAllLUsDF(appconf).cache()


    val allEntsDF =  getAllEnts(allLUsDF,appconf).cache()


    val allLOUs: Dataset[Row] = getAllLOUs(allEntsDF,appconf,Configs.conf).cache()


    saveLinks(allLOUs,allLUsDF,appconf)
    saveEnts(allEntsDF,appconf)
    saveLous(allLOUs,appconf)
    allLUsDF.unpersist()
    allLOUs.unpersist()
    }


    def getAllLUsDF(appconf: AppParams)(implicit spark: SparkSession) = {

      val incomingBiDataDF: DataFrame = getIncomingBiData(appconf)

      val existingLEsDF: DataFrame = getExistingLeusDF(appconf, Configs.conf)

      val joinedLUs = incomingBiDataDF.join(
        existingLEsDF.withColumnRenamed("ubrn", "id").select("id", "ern"),
        Seq("id"), "left_outer")

      getAllLUs(joinedLUs, appconf)

    }

    def getAllEnts(allLUsDF:DataFrame,appconf: AppParams)(implicit spark: SparkSession) = {

      val existingEntDF = getExistingEntsDF(appconf,Configs.conf)
      val newLEUsDF = allLUsDF.join(existingEntDF.select(col("ern")),Seq("ern"),"left_anti")
      val newEntsDF = spark.createDataFrame(createNewEnts(newLEUsDF).rdd,entRowSchema)
      val allEntsDF =  existingEntDF.union(newEntsDF)
      allEntsDF
    }


    def getAllLOUs(allEntsDF:DataFrame,appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {

      val existingLOUs: DataFrame = getExistingLousDF(appconf,confs)

      val entsWithoutLOUs: DataFrame = allEntsDF.join(existingLOUs.select("ern"),Seq("ern"),"left_anti")//.repartition(numOfPartitions)

      val newAndMissingLOUsDF: DataFrame =  createNewLOUs(entsWithoutLOUs,appconf)

      existingLOUs.union(newAndMissingLOUsDF)
    }


  }

object NewPeriodClosure extends NewPeriodClosure