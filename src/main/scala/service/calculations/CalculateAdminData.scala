package service.calculations

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import uk.gov.ons.registers.methods._
import util.configuration.AssemblerConfiguration

object CalculateAdminData extends PayeCalculator with VatCalculator with Serializable {

  @transient lazy val log: Logger = Logger.getLogger("EnterpriseAssembler")

  def apply(unitsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    log.debug("--> Start CalculateAdminData")

    val vatDF = spark.read.option("header", "true").csv(AssemblerConfiguration.PathToVat)
    val payeDF = spark.read.option("header", "true").csv(AssemblerConfiguration.PathToPaye)

    val payeCalculated: DataFrame = calculatePAYE(unitsDF, payeDF)

    val vatCalculated: Dataset[Row] = calculateVAT(unitsDF, payeCalculated, vatDF)

    log.debug("--> End CalculateAdminData")

    vatCalculated
  }

}
