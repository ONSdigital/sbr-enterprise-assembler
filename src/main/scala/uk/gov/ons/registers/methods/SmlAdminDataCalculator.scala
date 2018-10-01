package uk.gov.ons.registers.methods

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SmlAdminDataCalculator extends PayeCalculator with VatCalculator{

  def calculate(unitsDF:DataFrame, payeDF:DataFrame, vatDF:DataFrame)(implicit spark: SparkSession ) = {

    val payeCalculated:DataFrame = calculatePaye(unitsDF,payeDF)

    calculateVat(unitsDF,payeCalculated,vatDF)
  }

}
