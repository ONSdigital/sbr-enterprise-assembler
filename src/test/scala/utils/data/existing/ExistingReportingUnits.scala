package utils.data.existing

import model.domain.{HFileRow, KVCell}
import utils.data.TestIds

/**
  *
  */
trait ExistingReportingUnits {this:TestIds =>

  val existingRusForNewPeriodScenario = Seq(
    HFileRow(s"${entWithMissingLouId.reverse}~$missingLouRurn",List(KVCell("rurn",missingLouRurn),KVCell("ern",entWithMissingLouId),KVCell("prn","1111111111"),KVCell("name","INDUSTRIES LTD"),KVCell("entref","9900000009"),KVCell("address1","P O BOX 22"), KVCell("address2","INDUSTRIES HOUSE"), KVCell("address3","WHITE LANE"), KVCell("address4","REDDITCH"), KVCell("address5","WORCESTERSHIRE"), KVCell("postcode","B22 2TL"), KVCell("sic07","12345"), KVCell("employees","2"), KVCell("employment","1"),KVCell("turnover","300"))),
    HFileRow("1100000003~1000000001",List(KVCell("rurn","1000000001"),KVCell("ern","3000000011"),KVCell("prn","2222222222"),KVCell("name","2-ND LU OF BLACKWELLGROUP LTD"),KVCell("entref","9900000126"),KVCell("address1","North End Rd lane"), KVCell("address2","Croydon"), KVCell("address3","Surrey"), KVCell("employees","2"),  KVCell("lurn","300000088"),  KVCell("postcode","CR0 1AA"), KVCell("sic07","1122"), KVCell("employees","3"), KVCell("employment","2"),KVCell("turnover","400"))),
    HFileRow("1100000003~2000000002",List(KVCell("rurn","2000000002"),KVCell("ern","3000000011"),KVCell("prn","3333333333"),KVCell("name","BLACKWELLGROUP LTD"), KVCell("entref","9900000126"), KVCell("address1","GOGGESHALL ROAD"), KVCell("address2","EARLS COLNE"), KVCell("address3","COLCHESTER"), KVCell("employees","2"), KVCell("postcode","CO6 2JX"), KVCell("sic07","23456"), KVCell("employees","4"), KVCell("employment","3"),KVCell("turnover","500"))),
    HFileRow("1100000004~3000000003",List(KVCell("rurn","3000000003"),KVCell("ern","4000000011"),KVCell("prn","4444444444"),KVCell("name","3-RD LU OF IBM LTD"),KVCell("entref","9900000242"),KVCell("address1","IBM HOUSE"), KVCell("address2","Smile Street"), KVCell("address3","Cardiff"), KVCell("address4","SOUTH WALES"), KVCell("employees","1"),  KVCell("postcode","CF23 9EU"), KVCell("sic07","3344"), KVCell("employees","5"), KVCell("employment","4"),KVCell("turnover","600"))),
    HFileRow("1100000004~4000000004",List(KVCell("rurn","4000000004"),KVCell("ern","4000000011"),KVCell("prn","5555555555"),KVCell("name","2-ND LU OF IBM LTD"),KVCell("entref","9900000242"), KVCell("address1","IT DEPT"), KVCell("address2","1 Hight Street"), KVCell("address3","Newport"), KVCell("address4","SOUTH WALES"), KVCell("employees","2"), KVCell("postcode","NP10 6XG"), KVCell("sic07","2233"), KVCell("employees","6"), KVCell("employment","5"),KVCell("turnover","700"))),
    HFileRow("1100000004~5000000005",List(KVCell("rurn","5000000005"),KVCell("ern","4000000011"),KVCell("prn","6666666666"),KVCell("name","IBM LTD"),KVCell("entref","9900000242"),KVCell("address1","BSTER DEPT"), KVCell("address2","MAILPOINT A1F"), KVCell("address3","P O BOX 41"), KVCell("address4","NORTH HARBOUR"), KVCell("address5","PORTSMOUTH"), KVCell("employees","2"), KVCell("postcode","PO6 3AU"), KVCell("sic07","34567"), KVCell("employees","7"), KVCell("employment","6"),KVCell("turnover","800"))),
    HFileRow("1100000005~6000000006",List(KVCell("rurn","6000000006"),KVCell("ern","5000000011"),KVCell("prn","7777777777"),KVCell("name","MBI LTD"), KVCell("entref","9900000777"), KVCell("address1","99 Pen-Y-Lan Terrace"), KVCell("address2","Unit 11"), KVCell("address3","Cardiff"),KVCell("employees","5"),  KVCell("postcode","CF23 9EU"), KVCell("sic07","44044"), KVCell("employees","8"), KVCell("employment","7"),KVCell("turnover","900")))
  )

}
