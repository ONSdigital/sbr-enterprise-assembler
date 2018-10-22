package spark.calculations


import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.methods.VatCalculator



trait SmlAdminDataCalculator extends temp.calculations.methods.PayeCalculator with VatCalculator with Serializable{

  def calculate(unitsDF:DataFrame, payeDF:DataFrame, vatDF:DataFrame)(implicit spark: SparkSession ):DataFrame = {

    unitsDF.cache()

    val payeCalculated:DataFrame = calculatePAYE(unitsDF,payeDF)

    val res = calculateVAT(unitsDF,payeCalculated,vatDF)

    unitsDF.unpersist()
    res
  }

}
