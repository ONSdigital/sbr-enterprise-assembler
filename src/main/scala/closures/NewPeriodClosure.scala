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

  def addNewPeriodData(appconf: AppParams)(implicit spark: SparkSession, connection:Connection): Unit = {
    val confs = Configs.conf
    /**
      * Fields:
      * id,BusinessName, UPRN, PostCode,IndustryCode,LegalStatus,
      * TradingStatus, Turnover, EmploymentBands, PayeRefs, VatRefs, CompanyNo
      **/
    val incomingBiDataDF: DataFrame = getIncomingBiData(appconf)


    /**
      * id, ern
      **/
    val existingLEsDF: DataFrame = getExistingLeusDF(appconf, confs)

    /**
      * Assigns ERNs to existing LEUs
      * leaving ERN field with null values for new LEUs
      * Fields:
      * BusinessName, CompanyNo, IndustryCode, LegalStatus,
      * PayeRefs, PostCode, TradingStatus, Turnover, UPRN, VatRefs, id, ern
      **/
    val joinedLUs: DataFrame = incomingBiDataDF.join(existingLEsDF, Seq("id"), "left_outer")
    joinedLUs.cache()

    /**
      * Generate ERNs for new BI Units
      * Resulting dataset with all LEUs (new and existing) with erns
      * Fields:
      * ern, id, BusinessName, IndustryCode, LegalStatus, PostCode, TradingStatus Turnover, UPRN, CompanyNo, PayeRefs, VatRefs
      **/
    val allLUsDF: DataFrame = getAllLUs(joinedLUs, appconf)
    allLUsDF.cache()

    /**
      * ern, entref, name, trading_style, address1, address2, address3, address4, address5, postcode, sic07, legal_status
      **/
    val existingEntDF = getExistingEntsDF(appconf, confs)

    /**
      * ern, id, BusinessName, IndustryCode, LegalStatus, PostCode, TradingStatus Turnover, UPRN, CompanyNo, PayeRefs, VatRefs
      **/
    val newLEUsDF = allLUsDF.join(existingEntDF.select(col("ern")), Seq("ern"), "left_anti")

    /**
      * ern, entref, name, trading_style, address1, address2, address3, address4, address5, postcode, sic07, legal_status
      **/
    val newEntDF = spark.createDataFrame(createNewEntsWithoutCalculations(newLEUsDF).rdd, entRowSchema)


    val allEntsDF = existingEntDF.union(newEntDF)

    /**
      * Fields:
      * lurn, luref, ern, entref, name, tradingstyle, address1, address2, address3, address4, address5, postcode, sic07, employees
      **/

    val allLOUs: Dataset[Row] = getAllLOUs(allEntsDF, appconf, confs)
    //allLOUs.show()

    saveEnts(allEntsDF, appconf)
    saveLous(allLOUs, appconf)
    saveLinks(allLOUs, allLUsDF, appconf)
    allLUsDF.unpersist()
    joinedLUs.unpersist()
  }

  def getAllLOUs(allEntsDF: DataFrame, appconf: AppParams, confs: Configuration)(implicit spark: SparkSession, connection:Connection) = {

    val existingLOUs: DataFrame = getExistingLousDF(appconf, confs)
    val entsWithoutLOUs: DataFrame = allEntsDF.join(existingLOUs.select("ern"), Seq("ern"), "left_anti")
    val newAndMissingLOUsDF: DataFrame = createNewLOUs(entsWithoutLOUs,appconf)
    val allLOUs = existingLOUs.union(newAndMissingLOUsDF)

    allLOUs
  }

  /**
    * Creates new Enterprises from new LEUs with calculations
    * schema: completeNewEntSchema
    * ern, entref, name, address1, postcode, sic07, legal_status,
    * AND optional calculations:
    * paye_empees, paye_jobs, app_turnover, ent_turnover, cntd_turnover, std_turnover, grp_turnover
    **/
  def createNewEntsWithCalculations(newLEUsCalculatedDF: DataFrame)(implicit spark: SparkSession) = spark.createDataFrame(
    newLEUsCalculatedDF.rdd.map(row => Row(
      row.getAs[String]("ern"),
      Try {row.getAs[String]("entref")}.getOrElse(""),
      row.getAs[String]("BusinessName"),
      null, //trading_style
      Try {row.getAs[String]("address1")}.getOrElse(""),
      null, null, null, null, //address2,3,4,5
      row.getAs[String]("PostCode"),
      Try {row.getAs[String]("IndustryCode")}.getOrElse(""),
      row.getAs[String]("LegalStatus"),
      Try {row.getAs[String]("paye_employees")}.getOrElse(""),
      Try {row.getAs[String]("paye_jobs")}.getOrElse(""),
      Try {row.getAs[String]("apportioned_turnover")}.getOrElse(""),
      Try {row.getAs[String]("ent_turnover")}.getOrElse(""),
      Try {row.getAs[String]("contained_turnover")}.getOrElse(""),
      Try {row.getAs[String]("standard_turnover")}.getOrElse(""),
      Try {row.getAs[String]("group_turnover")}.getOrElse("")
    )
    ), completeEntSchema)

 /**
    * Creates new Enterprises from new LEUs without calculations
    * schema: completeNewEntSchema
    * ern, entref, name, address1, postcode, sic07, legal_status,
    **/
  def createNewEntsWithoutCalculations(newLEUs: DataFrame)(implicit spark: SparkSession) = spark.createDataFrame(
    newLEUs.rdd.map(row => Row(
      row.getAs[String]("ern"),
      Try {row.getAs[String]("entref")}.getOrElse(null),
      row.getAs[String]("BusinessName"),
      null, //trading_style
      Try {row.getAs[String]("address1")}.getOrElse(""),
      null, null, null, null, //address2,3,4,5
      row.getAs[String]("PostCode"),
      Try {row.getAs[String]("IndustryCode")}.getOrElse(""),
      row.getAs[String]("LegalStatus")
    )
    ), entRowSchema)


}

object NewPeriodClosure extends NewPeriodClosure