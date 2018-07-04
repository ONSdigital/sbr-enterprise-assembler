package test

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
  val existingRecordsDir = s"src/test/resources/data/$testDir/existing"
  val existingEntRecordHFiles = s"$existingRecordsDir/enterprise"
  val existingLinksRecordHFiles = s"$existingRecordsDir/links"
  val existingLousRecordHFiles = s"$existingRecordsDir/lou"
}
