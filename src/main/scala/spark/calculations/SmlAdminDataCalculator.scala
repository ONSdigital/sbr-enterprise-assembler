package spark.calculations


import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.methods._
import global.AppParams


trait SmlAdminDataCalculator extends PayeCalculator with VatCalculator with Serializable{

  def calculate(unitsDF:DataFrame, appconf:AppParams)(implicit spark: SparkSession ):DataFrame = {
    val vatDF = spark.read.option("header", "true").csv(appconf.PATH_TO_VAT)
    val payeDF = spark.read.option("header", "true").csv(appconf.PATH_TO_PAYE)

    val payeCalculated:DataFrame = calculatePAYE(unitsDF,payeDF)

    val vatCalculated = calculateVAT(unitsDF,payeCalculated,vatDF)

    vatCalculated
  }

}
