package spark.calculations




import global.AppParams
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.methods.{PAYE, VAT}



trait AdminDataCalculator {



  def calculate(unitsDF:DataFrame,appConfs:AppParams)(implicit spark: SparkSession ) = {
    val payeCalculator = new PAYE
    val vatCalculator = new VAT

    val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
    val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)

    val payeCalculated = payeCalculator.calculate(unitsDF,payeDF)

    vatCalculator.calculate(unitsDF,payeCalculated,vatDF)
  }

}
