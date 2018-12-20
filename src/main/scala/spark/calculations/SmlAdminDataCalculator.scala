package spark.calculations

import global.AppParams
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.methods._

trait SmlAdminDataCalculator extends PayeCalculator with VatCalculator with Serializable {

  def calculate(unitsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val vatDF = spark.read.option("header", "true").csv(AppParams.PATH_TO_VAT)
    val payeDF = spark.read.option("header", "true").csv(AppParams.PATH_TO_PAYE)

    val payeCalculated: DataFrame = calculatePAYE(unitsDF, payeDF)

    val vatCalculated = calculateVAT(unitsDF, payeCalculated, vatDF)

    vatCalculated
  }

}
