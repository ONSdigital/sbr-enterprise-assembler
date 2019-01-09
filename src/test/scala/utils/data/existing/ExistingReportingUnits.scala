package utils.data.existing

import model.{HFileRow, KVCell}
import utils.data.TestIds

/**
  *
  */
trait ExistingReportingUnits {
  this: TestIds =>

  val existingRusForNewPeriodScenario: Seq[HFileRow] = Seq(
    HFileRow(s"${entWithMissingLouId.reverse}~$missingLouRurn", List(KVCell("rurn", missingLouRurn),
      KVCell("ern", entWithMissingLouId), KVCell("entref", "9900000009"), KVCell("prn", "1111111111"), KVCell("legal_status", "3"), KVCell("trading_style", "A"), KVCell("name", "INDUSTRIES LTD"), KVCell("address1", "P O BOX 22"), KVCell("address2", "INDUSTRIES HOUSE"), KVCell("address3", "WHITE LANE"), KVCell("address4", "REDDITCH"), KVCell("address5", "WORCESTERSHIRE"), KVCell("postcode", "B22 2TL"), KVCell("region", ""), KVCell("sic07", "12345"), KVCell("employees", "2"), KVCell("employment", "4"), KVCell("turnover", "300"))),
    HFileRow("1100000003~2000000002", List(KVCell("rurn", "2000000002"), KVCell("ern", "3000000011"), KVCell("entref", "9900000126"), KVCell("prn", "3333333333"), KVCell("legal_status", "4"), KVCell("trading_style", "B"), KVCell("name", "BLACKWELLGROUP LTD"), KVCell("address1", "GOGGESHALL ROAD"), KVCell("address2", "EARLS COLNE"), KVCell("address3", "COLCHESTER"), KVCell("postcode", "CO6 2JX"), KVCell("region", "Colchester"), KVCell("sic07", "23456"), KVCell("employees", "4"), KVCell("employment", "6"), KVCell("turnover", "500"))),
    HFileRow("1100000004~5000000005", List(KVCell("rurn", "5000000005"), KVCell("ern", "4000000011"), KVCell("entref", "9900000242"), KVCell("prn", "6666666666"), KVCell("legal_status", "3"), KVCell("trading_style", "C"), KVCell("name", "IBM LTD"), KVCell("address1", "BSTER DEPT"), KVCell("address2", "MAILPOINT A1F"), KVCell("address3", "P O BOX 41"), KVCell("address4", "NORTH HARBOUR"), KVCell("address5", "PORTSMOUTH"), KVCell("postcode", "PO6 3AU"), KVCell("region", "Portsmouth"), KVCell("sic07", "34567"), KVCell("employees", "5"), KVCell("employment", "7"), KVCell("turnover", "800"))),
    HFileRow("1100000005~6000000006", List(KVCell("rurn", "6000000006"), KVCell("ern", "5000000011"), KVCell("entref", "9900000777"), KVCell("prn", "7777777777"), KVCell("legal_status", "1"), KVCell("trading_style", "U"), KVCell("name", "MBI LTD"), KVCell("address1", "99 Pen-Y-Lan Terrace"), KVCell("address2", "Unit 11"), KVCell("address3", "Cardiff"), KVCell("postcode", "CF23 9EU"), KVCell("region", "South Wales"), KVCell("sic07", "44044"), KVCell("employees", "5"), KVCell("employment", "5"), KVCell("turnover", "900")))
  )

}