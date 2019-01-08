package spark.calculations

import org.apache.spark.sql.{DataFrame, SparkSession}

trait AdminDataCalculator {

  def calculate(unitsDF: DataFrame)(implicit spark: SparkSession): DataFrame

}
