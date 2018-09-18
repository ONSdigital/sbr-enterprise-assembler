package global

/**
  * args sample:  LINKS ons l src/main/resources/data/links/hfile ENT ons d src/main/resources/data/enterprise/hfile src/main/resources/data/sample.parquet localhost 2181 201802 src/main/resources/data/smallPaye.csv src/main/resources/data/smallVat.csv local
  * args types:   linksTableName linksNameSpace linksTablefamily linksHfilePath enterpriseTableName enterpriseNameSpace enterpriseTableFamily enterpriseHFilePath parquetFilePath quorumHost quorumPort timePeriod payeCsvPath
  */


case class AppParams(
                      HBASE_LINKS_TABLE_NAME:String,
                      HBASE_LINKS_TABLE_NAMESPACE:String,
                      HBASE_LINKS_COLUMN_FAMILY:String,
                      PATH_TO_LINKS_HFILE:String,

                      HBASE_LEGALUNITS_TABLE_NAME:String,
                      HBASE_LEGALUNITS_TABLE_NAMESPACE:String,
                      HBASE_LEGALUNITS_COLUMN_FAMILY:String,
                      PATH_TO_LEGALUNITS_HFILE:String,

                      HBASE_ENTERPRISE_TABLE_NAME:String,
                      HBASE_ENTERPRISE_TABLE_NAMESPACE:String,
                      HBASE_ENTERPRISE_COLUMN_FAMILY:String,
                      PATH_TO_ENTERPRISE_HFILE:String,

                      HBASE_LOCALUNITS_TABLE_NAME:String,
                      HBASE_LOCALUNITS_TABLE_NAMESPACE:String,
                      HBASE_LOCALUNITS_COLUMN_FAMILY:String,
                      PATH_TO_LOCALUNITS_HFILE:String,

                      HBASE_REPORTINGUNITS_TABLE_NAME:String,
                      HBASE_REPORTINGUNITS_TABLE_NAMESPACE:String,
                      HBASE_REPORTINGUNITS_COLUMN_FAMILY:String,
                      PATH_TO_REPORTINGUNITS_HFILE:String,

                      PATH_TO_PARQUET:String,
                      TIME_PERIOD:String,
                      PATH_TO_PAYE:String,
                      PATH_TO_VAT:String,
                      PATH_TO_GEO:String,
                      ENV:String,
                      ACTION:String
                   ){
  val PREVIOUS_TIME_PERIOD = (TIME_PERIOD.toInt - 1).toString //temp
  val PATH_TO_LEU_TO_ENT_CSV = "src/main/resources/data/LeU_to_ENT_subset.csv"
}


object AppParams{

  def apply(args:Array[String]) = new AppParams(
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
                                                  args(25)
                                                 )
  
}
