package closures

import dao.hbase.{HBaseDao, HFileUtils}
import global.{AppParams, Configs}
import model.domain.HFileRow
import model.hfile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{NullType, StringType}
import spark.RddLogging
import spark.calculations.AdminDataCalculator
import spark.extensions.sql._

import scala.util.Try

trait NewPeriodWithCalculationsClosure extends AdminDataCalculator with BaseClosure with HFileUtils with RddLogging with Serializable{


  def addNewPeriodDataWithCalculations(appconf: AppParams)(implicit spark: SparkSession): Unit = {
    val confs = Configs.conf

    /**
      * Fields:
      * id,BusinessName, UPRN, PostCode,IndustryCode,LegalStatus,
      * TradingStatus, Turnover, EmploymentBands, PayeRefs, VatRefs, CompanyNo
      **/
    val incomingBiDataDF: DataFrame = getIncomingBiData(appconf)


    /**
      * ubrn, ern, CompanyNo, PayeRefs, VatRefs
      **/
    val existingLEsDF: DataFrame = getExistingLeusDF(appconf, confs)


    /**
      * Assigns ERNs to existing LEUs
      * leaving ERN field with null values for new LEUs
      * Fields:
      * BusinessName, CompanyNo, IndustryCode, LegalStatus,
      * PayeRefs, PostCode, TradingStatus, Turnover, UPRN, VatRefs, id, ern
      **/
    val joinedLUs = incomingBiDataDF.join(
      existingLEsDF.withColumnRenamed("ubrn", "id").select("id", "ern"),
      Seq("id"),
      "left_outer")
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
      * Calculates admin data
      * Fields:
      * ern,vatref, paye_employees, paye_jobs, contained_turnover, apportioned_turnover, standard_turnover
      **/
    val calculationsWithNumbers = calculate(allLUsDF,appconf)

    val calculatedDF = spark.createDataFrame(
                          calculationsWithNumbers.rdd.map(row => Row(
                            row.getAs[String]("ern"),
                            Try {
                              row.getAs[Long]("paye_empees")
                            }.map(_.toString).getOrElse(null),
                            Try {
                              row.getAs[Int]("paye_jobs")
                            }.map(_.toString).getOrElse(null),
                            Try {
                              row.getAs[Long]("app_turnover")
                            }.map(_.toString).getOrElse(null),
                            Try {
                              row.getAs[Long]("cntd_turnover")
                            }.map(_.toString).getOrElse(null),
                            Try {
                              row.getAs[Long]("ent_turnover")
                            }.map(_.toString).getOrElse(null),
                            Try {
                              row.getAs[Long]("std_turnover")
                            }.map(_.toString).getOrElse(null),
                            null //grp_turnover

                          )), calculationsSchema)


      calculatedDF.cache()


/**
  * ern, entref, name, trading_style, address1, address2, address3, address4, address5, postcode, sic07, legal_status
  * */
    val existingEntDF = getExistingEntsDF(appconf,confs)


    /**
      * ern, entref, name, trading_style, address1, address2, address3, address4, address5, postcode, sic07, legal_status
      * paye_employees, paye_jobs, contained_turnover, apportioned_turnover, standard_turnover
      * */
    val existingEntCalculatedDF = existingEntDF.join(calculatedDF,Seq("ern"), "left_outer").withColumn("grp_turnover", lit(null))
    //existingEntCalculatedDF.show()
    /**
      * ern, id, BusinessName, IndustryCode, LegalStatus, PostCode, TradingStatus Turnover, UPRN, CompanyNo, PayeRefs, VatRefs
      * */
    val newLEUsDF = allLUsDF.join(existingEntDF.select(col("ern")),Seq("ern"),"left_anti")

    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, Seq("ern"),"left_outer")
    /**
      * ern, entref, name, trading_style, address1, address2, address3, address4, address5, postcode, sic07, legal_status
      *
      * paye_empees, paye_jobs, app_turnover, ent_turnover, cntd_turnover, std_turnover, grp_turnover
      * */
    val newEntsCalculatedDF = spark.createDataFrame(createNewEnts(newLEUsCalculatedDF).rdd,completeEntSchema)
    //newEntsCalculatedDF.show()
    //val allEntsErns = allLUsDF.select(col("ern")).distinct

    val allEntsDF =  existingEntCalculatedDF.union(newEntsCalculatedDF)

/**
  * Fields:
  * lurn, luref, ern, entref, name, tradingstyle, address1, address2, address3, address4, address5, postcode, sic07, employees,
  * */

    val allLOUs: Dataset[Row] = getAllLOUs(allEntsDF,appconf,confs)
    allLOUs.show()
    saveLinks(allLOUs,allLUsDF,appconf)
    saveEnts(allEntsDF,appconf)
    saveLous(allLOUs,appconf)
    //calculatedDF.show()
    //calculatedDF.printSchema()
    allLUsDF.unpersist()
    joinedLUs.unpersist()
  }


  def getAllLOUs(allEntsDF:DataFrame,appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {

    val existingLOUs: DataFrame = getExistingLousDF(appconf,confs)

    val entsWithoutLOUs: DataFrame = allEntsDF.join(existingLOUs.select("ern"),Seq("ern"),"left_anti")

    val newAndMissingLOUsDF: DataFrame =  createNewAndMissingLOUs(entsWithoutLOUs,appconf)

    val allLOUs = existingLOUs.union(newAndMissingLOUsDF)
    //allLOUs.show()
    allLOUs
  }

//ern, entref, name, trading_style, address1, address2, address3, address4, address5, postcode, sic07, legal_status
  def createNewAndMissingLOUs(ents: DataFrame, appconf: AppParams)(implicit spark: SparkSession) = {

    spark.createDataFrame(
      ents.rdd.map(row => Row(
        generateLurn(row,appconf),
        Try {row.getAs[String]("luref")}.getOrElse(null),
        row.getAs[String]("ern"),
        row.getAs[String]("name"),
        Try {row.getAs[String]("entref")}.getOrElse(null),
        Try {row.getAs[String]("trading_style")}.getOrElse(null),
        row.getAs[String]("address1"),
        Try {row.getAs[String]("address2")}.getOrElse(null),
        Try {row.getAs[String]("address3")}.getOrElse (null),
        Try {row.getAs[String]("address4")}.getOrElse (null),
        Try {row.getAs[String]("address5")}.getOrElse (null),
        Try {
          try{
            row.getAs[String]("postcode")
          }catch {
            case e: Exception => {
              println(s"postcode is null with ern: ${row.getAs[String]("ern")}")
              throw e
            }
          }
        }.getOrElse ("NOT-PROVIDED"),
        row.getAs[String]("sic07"),
        Try {row.getAs[Long]("paye_empees")}.getOrElse("")
      )), louRowSchema)
  }
  /**
    * Creates new Enterprises from new LEUs with calculations
    * schema: completeNewEntSchema
    * ern, entref, name, address1, postcode, sic07, legal_status,
    * AND calculations:
    * paye_empees, paye_jobs, app_turnover, ent_turnover, cntd_turnover, std_turnover, grp_turnover
    * */
  def createNewEnts(newLEUsCalculatedDF:DataFrame)(implicit spark: SparkSession) = spark.createDataFrame(
    newLEUsCalculatedDF.rdd.map(row => Row(
      row.getAs[String]("ern"),
      Try {row.getAs[String]("entref")}.getOrElse(null),
      row.getAs[String]("BusinessName"),
      null, //trading_style
      Try {row.getAs[String]("address1")}.getOrElse(""),
      null, null, null, null, //address2,3,4,5
      row.getAs[String]("PostCode"),
      Try {row.getAs[String]("IndustryCode")}.getOrElse(""),
      row.getAs[String]("LegalStatus"),
      Try {row.getAs[String]("paye_empees")}.getOrElse(null),
      Try {row.getAs[String]("paye_jobs")}.getOrElse(null),
      Try {row.getAs[String]("app_turnover")}.getOrElse(null),
      Try {row.getAs[String]("ent_turnover")}.getOrElse(null),
      Try {row.getAs[String]("cntd_turnover")}.getOrElse(null),
      Try {row.getAs[String]("std_turnover")}.getOrElse(null),
      Try {row.getAs[String]("grp_turnover")}.getOrElse(null)
    )
    ), completeEntSchema)
/**
  * Fields:
  * id,BusinessName, UPRN, PostCode,IndustryCode,LegalStatus,
  * TradingStatus, Turnover, EmploymentBands, PayeRefs, VatRefs, CompanyNo
  * */
  def getIncomingBiData(appconf: AppParams)(implicit spark: SparkSession) = {
    val updatedConfs = appconf.copy(TIME_PERIOD = appconf.PREVIOUS_TIME_PERIOD)
    val parquetDF = spark.read.parquet(appconf.PATH_TO_PARQUET)
    parquetDF.castAllToString
  }

  def getAllLUs(joinedLUs: DataFrame,appconf:AppParams)(implicit spark: SparkSession) = {

    val rows = joinedLUs.rdd.map { row => {
      val ern = if (row.isNull("ern")) generateErn(row, appconf) else row.getAs[String]("ern")

      Row(
        ern,
        row.getAs[String]("id"),
        row.getAs[String]("BusinessName"),
        row.getAs[String]("IndustryCode"),
        row.getAs[String]("LegalStatus"),
        row.getAs[String]("PostCode"),
        row.getAs[String]("TradingStatus"),
        row.getAs[String]("Turnover"),
        row.getAs[String]("UPRN"),
        row.getAs[String]("CompanyNo"),
        row.getAs[Seq[String]]("PayeRefs"),
        row.getAs[Seq[String]]("VatRefs")
      )}}
    //printRddOfRows("rows",rows)
    spark.createDataFrame(rows, biWithErnSchema)
  }
}
object NewPeriodWithCalculationsClosure extends NewPeriodWithCalculationsClosure
