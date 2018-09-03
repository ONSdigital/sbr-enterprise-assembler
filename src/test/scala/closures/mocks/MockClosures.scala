package closures.mocks

import closures.BaseClosure
import dao.hbase.HFileUtils


trait MockClosures extends MockDataReader with MockUnitsIdGenerator{this:BaseClosure with HFileUtils =>

  val ernMapping: Map[String, String] = Map(
    ("5TH PROPERTY TRADING LIMITED" ->  "111111111-TEST-ERN"),
    ("ACCLAIMED HOMES LIMITED" ->       "222222222-TEST-ERN"),
    ("MERCATURA INVESTMENTS LIMITED" -> "333333333-TEST-ERN"),
    ("NEW ENTERPRISE LU" -> "444444444-TEST-ERN")
  )

  val lurnMapping: Map[String, String] = Map(
    ("5TH PROPERTY TRADING LIMITED" ->  "11111111-TEST-LURN"),
    ("ACCLAIMED HOMES LIMITED" ->       "22222222-TEST-LURN"),
    ("MERCATURA INVESTMENTS LIMITED" -> "33333333-TEST-LURN"),
    ("NEW ENTERPRISE LU" -> "444444444-TEST-LURN")
  )

  val rurnMapping: Map[String, String] = Map(
    ("5TH PROPERTY TRADING LIMITED" ->  "111111111-TEST-RURN"),
    ("ACCLAIMED HOMES LIMITED" ->       "222222222-TEST-RURN"),
    ("MERCATURA INVESTMENTS LIMITED" -> "333333333-TEST-RURN"),
    ("NEW ENTERPRISE LU" -> "444444444-TEST-RURN")
  )
  val prnMapping: Map[String, String] = Map(
    ("5TH PROPERTY TRADING LIMITED" ->  "111111111-TEST-PRN"),
    ("ACCLAIMED HOMES LIMITED" ->       "222222222-TEST-PRN"),
    ("MERCATURA INVESTMENTS LIMITED" -> "333333333-TEST-PRN"),
    ("NEW ENTERPRISE LU" -> "444444444-TEST-PRN")
  )

}

