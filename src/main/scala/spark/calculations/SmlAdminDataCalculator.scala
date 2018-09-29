package spark.calculations




import global.AppParams
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.methods.{PAYE, VAT}



trait SmlAdminDataCalculator{

  def calculate(unitsDF:DataFrame, appConfs:AppParams)(implicit spark: SparkSession ) = {

    val vatDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_VAT)
    val payeDF = spark.read.option("header", "true").csv(appConfs.PATH_TO_PAYE)

    val payeCalculated:DataFrame = PAYE.Paye.calculate(unitsDF,payeDF)

    VAT.Vat.calculate(unitsDF,payeCalculated,vatDF)
  }

}
