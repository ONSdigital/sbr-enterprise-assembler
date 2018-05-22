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

                        HBASE_ENTERPRISE_TABLE_NAME:String,
                        HBASE_ENTERPRISE_TABLE_NAMESPACE:String,
                        HBASE_ENTERPRISE_COLUMN_FAMILY:String,
                        PATH_TO_ENTERPRISE_HFILE:String,

                        HBASE_LOCALUNITS_TABLE_NAME:String,
                        HBASE_LOCALUNITS_TABLE_NAMESPACE:String,
                        HBASE_LOCALUNITS_COLUMN_FAMILY:String,
                        PATH_TO_LOCALUNITS_HFILE:String,

                        PATH_TO_PARQUET:String,
                        TIME_PERIOD:String,
                        PATH_TO_PAYE:String,
                        PATH_TO_VAT:String,
                        ENV:String,
                        ACTION:String
                   ){
  val PATH_TO_LINK_DELETE_PERIOD_HFILE = PATH_TO_LINKS_HFILE
  val PATH_TO_ENTERPRISE_DELETE_PERIOD_HFILE = PATH_TO_ENTERPRISE_HFILE


  val PATH_TO_LOCALUNITS_DELETE_PERIOD_HFILE = PATH_TO_LOCALUNITS_HFILE+"/cleanups"
  val PATH_TO_LINKS_HFILE_DELETE = PATH_TO_LINKS_HFILE+"/cleanups"
  val PATH_TO_LINKS_HFILE_UPDATE = PATH_TO_LINKS_HFILE + "/updates"
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
                                                  args(17)
                                                )
  
}
