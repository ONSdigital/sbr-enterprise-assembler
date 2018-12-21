package spark.calculations

import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.methods._
import util.options.ConfigOptions

trait SmlAdminDataCalculator extends PayeCalculator with VatCalculator with Serializable {

  def calculate(unitsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val vatDF = spark.read.option("header", "true").csv(ConfigOptions.PathToVat)
    val payeDF = spark.read.option("header", "true").csv(ConfigOptions.PathToPaye)

    val payeCalculated: DataFrame = calculatePAYE(unitsDF, payeDF)

    val vatCalculated = calculateVAT(unitsDF, payeCalculated, vatDF)

    vatCalculated
  }

}
