package closures.mocks

import closures.CreateNewPeriodClosure
import dao.hbase.MockCreateNewPeriodHBaseDao

import scala.util.Random

object MockCreateNewPeriodClosure extends CreateNewPeriodClosure{
  override val hbaseDao = MockCreateNewPeriodHBaseDao
  override def generateUniqueKey = Random.alphanumeric.take(12).mkString + "TESTS" //to ensure letters and numbers present

}
