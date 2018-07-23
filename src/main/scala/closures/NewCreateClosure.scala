package closures

import dao.hbase.HBaseDao
import dao.hbase.converter.{UnitRowConverter, WithConversionHelper}
import global.{AppParams, Configs}
import model.domain.HFileRow
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import spark.RddLogging
import spark.calculations.{AdminDataCalculator, DataFrameHelper}
import spark.extensions.sql._

import scala.util.Try

trait NewCreateClosure extends WithConversionHelper with UnitRowConverter with DataFrameHelper with AdminDataCalculator with RddLogging with Serializable{

  val hbaseDao: HBaseDao = HBaseDao

  def addNewPeriodData(appconf: AppParams)(implicit spark: SparkSession): Unit = {
    val confs = Configs.conf
    
    /**
      * Fields:
      * id,BusinessName, UPRN, PostCode,IndustryCode,LegalStatus,   
      * TradingStatus, Turnover, EmploymentBands, PayeRefs, VatRefs, CompanyNo
      * */
    val incomingBiDataDF: DataFrame = getIncomingBiData(appconf)
    
    
    /**
      * id, ern
      * */
    val existingLEsDF: DataFrame = getExistingLEs(appconf,confs)


    /**
      * Assigns ERNs to existing LEUs
      * leaving ERN field with null values for new LEUs
      * Fields:
      * BusinessName, CompanyNo, IndustryCode, LegalStatus,
      * PayeRefs, PostCode, TradingStatus, Turnover, UPRN, VatRefs, id, ern
      * */
    val joinedLUs = existingLEsDF.join(incomingBiDataDF,col("id"),"full_outer")
    joinedLUs.cache()


  /**
    * Generate ERNs for new BI Units
    * Resulting dataset with all LEUs (new and existing) with erns
    * Fields:
    * ern, id, BusinessName, IndustryCode, LegalStatus, PostCode, TradingStatus Turnover, UPRN, CompanyNo, PayeRefs, VatRefs
    * */
    val allLUsDF: DataFrame = getAllLUs(joinedLUs)
    allLUsDF.cache()

    /**
      * Calculates admin data
      * Fields:
      * ern,vatref, paye_employees, paye_jobs, contained_turnover, apportioned_turnover, standard_turnover
      * */
    val calculatedDF = AdminDataCalculator.calculate(allLUsDF,appconf).cache()
    
    
/**
  * ern, entref, name, trading_style, address1, address2, address3, address4, address5, postcode, sic07, legal_status
  * */
    val existingEntDF = getExistingEntsDF(appconf,confs)

    val existingEntCalculatedDF = existingEntDF.join(calculatedDF,col("ern"), "left_outer")

    /**
      * ern, id, BusinessName, IndustryCode, LegalStatus, PostCode, TradingStatus Turnover, UPRN, CompanyNo, PayeRefs, VatRefs
      * */
    val newLEUsDF = allLUsDF.join(existingEntDF.select(col("ern")),col("ern"),"left_anti")

    val newLEUsCalculatedDF = newLEUsDF.join(calculatedDF, col("ern"),"left_outer")
    /**
      * ern, entref, name, address1, postcode, sic07, legal_status,
      *
      * paye_empees, paye_jobs, app_turnover, ent_turnover, cntd_turnover, std_turnover, grp_turnover
      * */
    val newEntsCalculatedDF = spark.createDataFrame(createNewEnts(newLEUsCalculatedDF).rdd,completeNewEntSchema)

    val allEntsErns = allLUsDF.select(col("ern")).distinct

    val allEntsDF =  existingEntCalculatedDF.union(newEntsCalculatedDF)
    //@ this point :
    //1. allLUsDF
    //2. newEntsCalculated
    //3. existingEntCalculatedDF

    /*what can be done with this:
    * allLUsDF will populate LINKS with all data needed except ~ENT~'s LOU related cells
    * newEntsCalculated & existingEntCalculatedDF will fully populate ENT table
    * */

    /*
    * What is missing :
    * data for adding cells with LURNs of related LOUs to LINKS
    * data for fully populating LOU table
    * (Basically everything to do with LOU is missing)
    * */
/**
  * Fields:
  * lurn, luref, ern, entref, name, tradingstyle, address1, address2, address3, address4, address5, postcode, sic07, employees,
  * */

    val allLOUs = getAllLOUs(allEntsDF,appconf,confs)
   //Summary:
   // 1. allLOUs
   // 2. allEntsDF
   // 3. allLUsDF

    //calculatedDF.show()
    //calculatedDF.printSchema()
    allLUsDF.unpersist()
    joinedLUs.unpersist()
  }





  def getAllLOUs(allEntsDF:DataFrame,appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    val existingLOUs: DataFrame = getExistingLOUsDF(appconf,confs)

    val entsWithoutLOUs: DataFrame = allEntsDF.join(existingLOUs.select("ern"),col("ern"),"left_anti")

    val newAndMissingLOUsDF: DataFrame =  createNewAndMissingLOUs(entsWithoutLOUs)

    val allLOUs = existingLOUs.union(newAndMissingLOUsDF)

    allLOUs
  }


//ern, entref, name, trading_style, address1, address2, address3, address4, address5, postcode, sic07, legal_status
  def createNewAndMissingLOUs(ents: DataFrame)(implicit spark: SparkSession) = spark.createDataFrame(
    ents.map(row => Row(Array(
                                                      row.getAs[String]("lurn"),
                                                      Try{row.getAs[String]("luref")}.getOrElse(null),
                                                      row.getAs[String]("ern"),
                                                      row.getAs[String]("entref"),
                                                      row.getAs[String]("name"),
                                                      Try{row.getAs[String]("tradingstyle")}.getOrElse(null),
                                                      row.getAs[String]("address1"),
                                                      Try{row.getAs[String]("address2")}.getOrElse(null),
                                                      Try{row.getAs[String]("address3")}getOrElse(null),
                                                      Try{row.getAs[String]("address4")}getOrElse(null),
                                                      Try{row.getAs[String]("address5")}getOrElse(null),
                                                      Try{row.getAs[String]("postcode")}getOrElse(null),
                                                      row.getAs[String]("sic07"),
                                                      row.getAs[String]("paye_employees")
                                                  ))).rdd,louRowSchema
/*
* lurn
  luref
  ern
  entref
  name
  tradingstyle
  address1
  address2
  address3
  address4
  address5
  postcode
  sic07
  employees*/

  )
  /**
    * Creates new Enterprises from new LEUs with calculations
    * schema: completeNewEntSchema
    * ern, entref, name, address1, postcode, sic07, legal_status,
    * AND calculations:
    * paye_empees, paye_jobs, app_turnover, ent_turnover, cntd_turnover, std_turnover, grp_turnover
    * */
  def createNewEnts(newLEUsCalculatedDF:DataFrame)(implicit spark: SparkSession) = spark.createDataFrame(
    newLEUsCalculatedDF.map(row =>  Row(Array(
                                                                  row.getAs[String]("ern"),
                                                                  Try{row.getAs[String]("entref") }.getOrElse(""),
                                                                  row.getString("BusinessName"),
                                                                  row.getString("PostCode"),
                                                                  Try{row.getString("IndustryCode").get}.getOrElse(""),
                                                                  Try{row.getString("address1").get}.getOrElse(""),
                                                                  row.getString("LegalStatus"),
                                                                  row.getString("paye_employees"),
                                                                  row.getString("paye_jobs"),
                                                                  row.getString("apportioned_turnover"),
                                                                  row.getString("ent_turnover"),
                                                                  row.getString("contained_turnover"),
                                                                  row.getString("standard_turnover"),
                                                                  row.getString("group_turnover")
                                                                ))
                              ).rdd,completeNewEntSchema)
  
  def getExistingLOUsDF(appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    val localUnitsTableName = s"${appconf.HBASE_LOCALUNITS_TABLE_NAMESPACE}:${appconf.HBASE_LOCALUNITS_TABLE_NAME}"
    val regex = ".*~"+{appconf.PREVIOUS_TIME_PERIOD}+"~.*"
    val louHFileRowRdd: RDD[HFileRow] = hbaseDao.readTableWithKeyFilter(confs,appconf, localUnitsTableName, regex)
    val existingLouRdd: RDD[Row] = louHFileRowRdd.map(_.toLouRow)
    spark.createDataFrame(existingLouRdd,louRowSchema)   
  }

  def getExistingEntsDF(appconf: AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    val entRegex = ".*~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val entTableName = s"${appconf.HBASE_ENTERPRISE_TABLE_NAMESPACE}:${appconf.HBASE_ENTERPRISE_TABLE_NAME}"
    val entHFileRowRdd: RDD[HFileRow] = hbaseDao.readTableWithKeyFilter(confs,appconf, entTableName, entRegex)
    val existingEntRdd: RDD[Row] = entHFileRowRdd.map(_.toEntRow)
    spark.createDataFrame(existingEntRdd,entRowSchema)
  }

/**
  * returns existing ~LEU~ links DF
  * fields:
  * id:String,
  * ern:String
  * */
  def getExistingLEs(appconf:AppParams,confs:Configuration)(implicit spark: SparkSession) = {
    val linksTableName = s"${appconf.HBASE_LINKS_TABLE_NAMESPACE}:${appconf.HBASE_LINKS_TABLE_NAME}"
    val luRegex = ".*(LEU)~"+{appconf.PREVIOUS_TIME_PERIOD}+"$"
    val existingLinks: RDD[Row] = hbaseDao.readTableWithKeyFilter(confs,appconf, linksTableName, luRegex).map(_.toUbrnErnRow)
    val existingLinksDF:DataFrame = spark.createDataFrame(existingLinks,existingLuBiRowSchema)
    existingLinksDF
  }
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

  def getAllLUs(joinedLUs: DataFrame)(implicit spark: SparkSession) = {
    val dataset: Dataset[Row] = joinedLUs.map{
      case row if(Try{row.getAs[String]("ern")}.isFailure) => {
        val ern = generateErn(null,null)
        Row(Array(
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
        ))

      }}
    spark.createDataFrame(dataset.rdd,biWithErnSchema)
  }
}
object NewCreateClosure extends NewCreateClosure
