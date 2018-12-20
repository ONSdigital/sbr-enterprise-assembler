package global

private case class AppParameters(
                          HBASE_LINKS_TABLE_NAME: String,
                          HBASE_LINKS_TABLE_NAMESPACE: String,
                          HBASE_LINKS_COLUMN_FAMILY: String,
                          PATH_TO_LINKS_HFILE: String,

                          HBASE_LEGALUNITS_TABLE_NAME: String,
                          HBASE_LEGALUNITS_TABLE_NAMESPACE: String,
                          HBASE_LEGALUNITS_COLUMN_FAMILY: String,
                          PATH_TO_LEGALUNITS_HFILE: String,

                          HBASE_ENTERPRISE_TABLE_NAME: String,
                          HBASE_ENTERPRISE_TABLE_NAMESPACE: String,
                          HBASE_ENTERPRISE_COLUMN_FAMILY: String,
                          PATH_TO_ENTERPRISE_HFILE: String,

                          HBASE_LOCALUNITS_TABLE_NAME: String,
                          HBASE_LOCALUNITS_TABLE_NAMESPACE: String,
                          HBASE_LOCALUNITS_COLUMN_FAMILY: String,
                          PATH_TO_LOCALUNITS_HFILE: String,

                          HBASE_REPORTINGUNITS_TABLE_NAME: String,
                          HBASE_REPORTINGUNITS_TABLE_NAMESPACE: String,
                          HBASE_REPORTINGUNITS_COLUMN_FAMILY: String,
                          PATH_TO_REPORTINGUNITS_HFILE: String,

                          PATH_TO_PARQUET: String,
                          TIME_PERIOD: String,

                          HIVE_DB_NAME: String,
                          HIVE_TABLE_NAME: String,
                          HIVE_SHORT_TABLE_NAME: String,

                          PATH_TO_PAYE: String,
                          PATH_TO_VAT: String,
                          ENV: String,
                          ACTION: String
                        ) {
  val PREVIOUS_TIME_PERIOD: String = (TIME_PERIOD.toInt - 1).toString //temp
  val PATH_TO_LEU_TO_ENT_CSV: String = "src/main/resources/data/LeU_to_ENT_subset.csv"

  val DEFAULT_GEO_PATH: String = "src/main/resources/data/geo/test-dataset.csv"
  val DEFAULT_GEO_PATH_SHORT: String = "src/main/resources/data/geo/test_short-dataset.csv"

  val PATH_TO_GEO: String = DEFAULT_GEO_PATH
  val PATH_TO_GEO_SHORT: String = DEFAULT_GEO_PATH_SHORT
}

private object AppParameters {

  def apply(args: Array[String]) = new AppParameters(
    args(0),
    args(1),
    args(2),
    args(3),
    args(4),
    args(5),
    args(6),
    args(7),
    args(8),
    args(9),
    args(10),
    args(11),
    args(12),
    args(13),
    args(14),
    args(15),
    args(16),
    args(17),
    args(18),
    args(19),
    args(20),
    args(21),
    args(22),
    args(23),
    args(24),
    args(25),
    args(26),
    args(27),
    args(28)
  )

}
