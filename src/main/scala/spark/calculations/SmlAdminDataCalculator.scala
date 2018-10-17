package spark.calculations


import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.methods.{PayeCalculator, VatCalculator}



trait SmlAdminDataCalculator extends PayeCalculator with VatCalculator with Serializable{

  def calculate(unitsDF:DataFrame, payeDF:DataFrame, vatDF:DataFrame)(implicit spark: SparkSession ) = {

    unitsDF.cache()

    val payeCalculated:DataFrame = calculatePAYE(unitsDF,payeDF)

    calculateVAT(unitsDF,payeCalculated,vatDF)

    unitsDF.unpersist()
  }

}
