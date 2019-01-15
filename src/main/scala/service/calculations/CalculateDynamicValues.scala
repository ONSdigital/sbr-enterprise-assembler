package service.calculations

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import util.configuration.AssemblerConfiguration

object CalculateDynamicValues {

  @transient lazy val log: Logger = Logger.getLogger("EnterpriseAssembler")

  /**
    * expects df with fields 'legal_status', 'postcode', 'paye_empees', 'working_props'
    **/
  def apply(df: DataFrame, regionsByPostcodeDF: DataFrame, regionsByPostcodeShortDF: DataFrame)
               (implicit spark: SparkSession): Dataset[Row] = {
    log.debug("--> Start CalculateDynamicValues")
    val partitions = spark.sparkContext.defaultParallelism
    val withWorkingProps = calculateWorkingProps(df).coalesce(partitions)
    val withEmployment = CalculateEmployment(withWorkingProps).coalesce(partitions)
    val res = CalculateRegion(withEmployment, regionsByPostcodeDF, regionsByPostcodeShortDF)
    log.debug("--> End CalculateDynamicValues")
    res
}

  private def calculateWorkingProps(dfWithLegalStatus: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import dfWithLegalStatus.sqlContext.implicits.StringToColumn
    import org.apache.spark.sql.functions.udf

    def calculation = udf((legalStatus: String) => getWorkingPropsByLegalStatus(legalStatus))

    dfWithLegalStatus.withColumn("working_props", calculation($"legal_status"))
  }

  private def getWorkingPropsByLegalStatus(legalStatus: String): String = legalStatus match {
    case "2" => "1"
    case "3" => "2"
    case _ => AssemblerConfiguration.DefaultWorkingProps
  }

}
