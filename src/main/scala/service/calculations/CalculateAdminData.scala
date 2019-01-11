package service.calculations

import java.util.Calendar

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import uk.gov.ons.registers.methods._
import util.configuration.AssemblerConfiguration

object CalculateAdminData extends PayeCalculator with VatCalculator with Serializable {



  def apply(unitsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val partitions = spark.sparkContext.defaultParallelism

    val vatDF = spark.read.option("header", "true").csv(AssemblerConfiguration.PathToVat)
    val payeDF = spark.read.option("header", "true").csv(AssemblerConfiguration.PathToPaye)

    val payeCalculated: DataFrame = calculatePAYE(unitsDF, payeDF).repartition(partitions)
    println(s"${Calendar.getInstance.getTime} --> payeCalculated")

    val vatCalculated: Dataset[Row] = calculateVAT(unitsDF, payeCalculated, vatDF).coalesce(partitions)
    println(s"${Calendar.getInstance.getTime} --> vatCalculated")

    vatCalculated
  }

}
