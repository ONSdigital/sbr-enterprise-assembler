package service.calculations

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, substring, trim}
import util.configuration.AssemblerConfiguration

object CalculateRegion {

  @transient lazy val log: Logger = Logger.getLogger("EnterpriseAssembler")

  def apply(dfWithPostcode: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame)
                     (implicit spark: SparkSession): Dataset[Row] = {

    log.debug("--> Start CalculateRegion")
    val step1DF = dfWithPostcode.drop("region")
    val step2DF = step1DF.join(regionsByPostcodeDF, Seq("postcode"), "left_outer")
    val step3DF = step2DF.select("*").where("region IS NULL")
    val partial = step2DF.select("*").where("region IS NOT NULL")
    val step4DF = step3DF.drop("region")
    val step5DF = step4DF.select(col("*"), trim(substring(col("postcode"), 0, col("postcode").toString().length - 4)).as("postcodeout"))
    val step6DF = step5DF.join(regionsByPostcodeShortDF, Seq("postcodeout"), "left_outer")
    val step7DF = step6DF.drop("postcodeout")
    val step8DF = step7DF.union(partial)
    val step9DF = step8DF.na.fill(AssemblerConfiguration.DefaultRegion, Seq("region"))
    log.debug("--> End CalculateRegion")
    step9DF
  }

}
