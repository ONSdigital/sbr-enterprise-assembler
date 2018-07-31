package utils.data.existing

import model.domain.{HFileRow, KVCell}
import utils.data.TestIds

trait ExistingLocalUnits {this:TestIds =>

val louForLouMissingScenario = Seq(
  HFileRow("1100000003~201803~300000088",List(KVCell("address1","North End Rd lane"), KVCell("address2","Croydon"), KVCell("address3","Surrey"), KVCell("employees","2"), KVCell("entref","9900000126"), KVCell("ern","3000000011"), KVCell("luref","100000827984"), KVCell("lurn","300000088"), KVCell("name","2-ND LU OF BLACKWELLGROUP LTD"), KVCell("postcode","CR0 1AA"), KVCell("sic07","1122"), KVCell("trading_style","B"))),
  HFileRow("1100000003~201803~300000099",List(KVCell("address1","GOGGESHALL ROAD"), KVCell("address2","EARLS COLNE"), KVCell("address3","COLCHESTER"), KVCell("employees","2"), KVCell("entref","9900000126"), KVCell("ern","3000000011"), KVCell("luref","100000246017"), KVCell("lurn","300000099"), KVCell("name","BLACKWELLGROUP LTD"), KVCell("postcode","CO6 2JX"), KVCell("sic07","23456"), KVCell("trading_style","B"))),
  HFileRow("1100000004~201803~400000055",List(KVCell("address1","IBM HOUSE"), KVCell("address2","Smile Street"), KVCell("address3","Cardiff"), KVCell("address4","SOUTH WALES"), KVCell("employees","1"), KVCell("entref","9900000242"), KVCell("ern","4000000011"), KVCell("luref","100000508724"), KVCell("lurn","400000055"), KVCell("name","3-RD LU OF IBM LTD"), KVCell("postcode","CF23 9EU"), KVCell("sic07","3344"), KVCell("trading_style","B"))),
  HFileRow("1100000004~201803~400000066",List(KVCell("address1","IT DEPT"), KVCell("address2","1 Hight Street"), KVCell("address3","Newport"), KVCell("address4","SOUTH WALES"), KVCell("employees","2"), KVCell("entref","9900000242"), KVCell("ern","4000000011"), KVCell("luref","100000508723"), KVCell("lurn","400000066"), KVCell("name","2-ND LU OF IBM LTD"), KVCell("postcode","NP10 6XG"), KVCell("sic07","2233"), KVCell("trading_style","A"))),
  HFileRow("1100000004~201803~400000077",List(KVCell("address1","BSTER DEPT"), KVCell("address2","MAILPOINT A1F"), KVCell("address3","P O BOX 41"), KVCell("address4","NORTH HARBOUR"), KVCell("address5","PORTSMOUTH"), KVCell("employees","2"), KVCell("entref","9900000242"), KVCell("ern","4000000011"), KVCell("luref","100000459235"), KVCell("lurn","400000077"), KVCell("name","IBM LTD"), KVCell("postcode","PO6 3AU"), KVCell("sic07","34567"), KVCell("trading_style","C")))
 )

 val existingLousForNewPeriodScenario = Seq(
  HFileRow(s"${entWithMissingLouId.reverse}~201803~$missingLouLurn",List(KVCell("address1","P O BOX 22"), KVCell("address2","INDUSTRIES HOUSE"), KVCell("address3","WHITE LANE"), KVCell("address4","REDDITCH"), KVCell("address5","WORCESTERSHIRE"), KVCell("employees","2"), KVCell("entref","9900000009"), KVCell("ern",entWithMissingLouId), KVCell("luref","100002826247"), KVCell("lurn",missingLouLurn), KVCell("name","INDUSTRIES LTD"), KVCell("postcode","B22 2TL"), KVCell("sic07","12345"), KVCell("trading_style","A"))),
  HFileRow("1100000003~201803~300000088",List(KVCell("address1","North End Rd lane"), KVCell("address2","Croydon"), KVCell("address3","Surrey"), KVCell("employees","2"), KVCell("entref","9900000126"), KVCell("ern","3000000011"), KVCell("luref","100000827984"), KVCell("lurn","300000088"), KVCell("name","2-ND LU OF BLACKWELLGROUP LTD"), KVCell("postcode","CR0 1AA"), KVCell("sic07","1122"), KVCell("trading_style","B"))),
  HFileRow("1100000003~201803~300000099",List(KVCell("address1","GOGGESHALL ROAD"), KVCell("address2","EARLS COLNE"), KVCell("address3","COLCHESTER"), KVCell("employees","2"), KVCell("entref","9900000126"), KVCell("ern","3000000011"), KVCell("luref","100000246017"), KVCell("lurn","300000099"), KVCell("name","BLACKWELLGROUP LTD"), KVCell("postcode","CO6 2JX"), KVCell("sic07","23456"), KVCell("trading_style","B"))),
  HFileRow("1100000004~201803~400000055",List(KVCell("address1","IBM HOUSE"), KVCell("address2","Smile Street"), KVCell("address3","Cardiff"), KVCell("address4","SOUTH WALES"), KVCell("employees","1"), KVCell("entref","9900000242"), KVCell("ern","4000000011"), KVCell("luref","100000508724"), KVCell("lurn","400000055"), KVCell("name","3-RD LU OF IBM LTD"), KVCell("postcode","CF23 9EU"), KVCell("sic07","3344"), KVCell("trading_style","B"))),
  HFileRow("1100000004~201803~400000066",List(KVCell("address1","IT DEPT"), KVCell("address2","1 Hight Street"), KVCell("address3","Newport"), KVCell("address4","SOUTH WALES"), KVCell("employees","2"), KVCell("entref","9900000242"), KVCell("ern","4000000011"), KVCell("luref","100000508723"), KVCell("lurn","400000066"), KVCell("name","2-ND LU OF IBM LTD"), KVCell("postcode","NP10 6XG"), KVCell("sic07","2233"), KVCell("trading_style","A"))),
  HFileRow("1100000004~201803~400000077",List(KVCell("address1","BSTER DEPT"), KVCell("address2","MAILPOINT A1F"), KVCell("address3","P O BOX 41"), KVCell("address4","NORTH HARBOUR"), KVCell("address5","PORTSMOUTH"), KVCell("employees","2"), KVCell("entref","9900000242"), KVCell("ern","4000000011"), KVCell("luref","100000459235"), KVCell("lurn","400000077"), KVCell("name","IBM LTD"), KVCell("postcode","PO6 3AU"), KVCell("sic07","34567"), KVCell("trading_style","C"))),
  HFileRow("1100000005~201803~550000088",List(KVCell("address1","99 Pen-Y-Lan Terrace"), KVCell("address2","Unit 11"), KVCell("address3","Cardiff"),KVCell("employees","5"), KVCell("entref","9900000777"), KVCell("ern","5000000011"), KVCell("luref","100000601835"), KVCell("lurn","550000088"), KVCell("name","MBI LTD"), KVCell("postcode","CF23 9EU"), KVCell("sic07","44044"), KVCell("trading_style","U")))
 )

}
