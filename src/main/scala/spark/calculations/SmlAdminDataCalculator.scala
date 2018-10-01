package spark.calculations



import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.methods.{PayeCalculator, VatCalculator}



trait SmlAdminDataCalculator extends PayeCalculator with VatCalculator{

  def calculate(unitsDF:DataFrame, payeDF:DataFrame, vatDF:DataFrame)(implicit spark: SparkSession ) = {

    val payeCalculated:DataFrame = calculatePaye(unitsDF,payeDF)

    calculateVat(unitsDF,payeCalculated,vatDF)
  }

}
