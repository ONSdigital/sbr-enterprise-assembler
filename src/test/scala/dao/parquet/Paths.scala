package dao.parquet

/**
  *
  */
trait Paths {this: { val testDir: String } =>

  def testDir:String

  val jsonFilePath = s"src/test/resources/data/$testDir/newPeriod.json"
  val linkHfilePath = s"src/test/resources/data/$testDir/links"
  val entHfilePath = s"src/test/resources/data/$testDir/enterprise"
  val louHfilePath = s"src/test/resources/data/$testDir/lou"
  val parquetPath = s"src/test/resources/data/$testDir/sample.parquet"
  val payeFilePath = s"src/test/resources/data/$testDir/newPeriodPaye.csv"
  val vatFilePath = s"src/test/resources/data/$testDir/newPeriodVat.csv"
  val existingEntRecordHFiles = s"src/test/resources/data/$testDir/existing/enterprise"
  val existingLinksRecordHFiles = s"src/test/resources/data/$testDir/existing/links"
  val existingLousRecordHFiles = s"src/test/resources/data/$testDir/existing/lou"
}
