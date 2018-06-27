package dao.parquet

import model.domain.{HFileRow, KVCell}

trait ExistingLocalUnits {
val louForLouMissingScenario = Seq(
  HFileRow("1100000003~201803~300000088",List(KVCell("address1","North End Rd lane"), KVCell("address2","Croydon"), KVCell("address3","Surrey"), KVCell("employees","2"), KVCell("entref","9900000126"), KVCell("ern","3000000011"), KVCell("luref","100000827984"), KVCell("lurn","300000088"), KVCell("name","2-ND LU OF BLACKWELLGROUP LTD"), KVCell("postcode","CR0 1AA"), KVCell("sic07","1122"), KVCell("trading_style","B"))),
  HFileRow("1100000003~201803~300000099",List(KVCell("address1","GOGGESHALL ROAD"), KVCell("address2","EARLS COLNE"), KVCell("address3","COLCHESTER"), KVCell("employees","2"), KVCell("entref","9900000126"), KVCell("ern","3000000011"), KVCell("luref","100000246017"), KVCell("lurn","300000099"), KVCell("name","BLACKWELLGROUP LTD"), KVCell("postcode","CO6 2JX"), KVCell("sic07","23456"), KVCell("trading_style","B"))),
  HFileRow("1100000004~201803~400000055",List(KVCell("address1","IBM HOUSE"), KVCell("address2","Smile Street"), KVCell("address3","Cardiff"), KVCell("address4","SOUTH WALES"), KVCell("employees","1"), KVCell("entref","9900000242"), KVCell("ern","4000000011"), KVCell("luref","100000508724"), KVCell("lurn","400000055"), KVCell("name","3-RD LU OF IBM LTD"), KVCell("postcode","CF23 9EU"), KVCell("sic07","3344"), KVCell("trading_style","B"))),
  HFileRow("1100000004~201803~400000066",List(KVCell("address1","IT DEPT"), KVCell("address2","1 Hight Street"), KVCell("address3","Newport"), KVCell("address4","SOUTH WALES"), KVCell("employees","2"), KVCell("entref","9900000242"), KVCell("ern","4000000011"), KVCell("luref","100000508723"), KVCell("lurn","400000066"), KVCell("name","2-ND LU OF IBM LTD"), KVCell("postcode","NP10 6XG"), KVCell("sic07","2233"), KVCell("trading_style","A"))),
  HFileRow("1100000004~201803~400000077",List(KVCell("address1","BSTER DEPT"), KVCell("address2","MAILPOINT A1F"), KVCell("address3","P O BOX 41"), KVCell("address4","NORTH HARBOUR"), KVCell("address5","PORTSMOUTH"), KVCell("employees","2"), KVCell("entref","9900000242"), KVCell("ern","4000000011"), KVCell("luref","100000459235"), KVCell("lurn","400000077"), KVCell("name","IBM LTD"), KVCell("postcode","PO6 3AU"), KVCell("sic07","34567"), KVCell("trading_style","C")))
 )
}
/*

 1100000004~201803~400000066    column=d:address1, timestamp=1530011497008, value=IT DEPT
 1100000004~201803~400000066    column=d:address2, timestamp=1530011497010, value=1 Hight Street
 1100000004~201803~400000066    column=d:address3, timestamp=1530011497011, value=Newport
 1100000004~201803~400000066    column=d:address4, timestamp=1530011497013, value=SOUTH WALES
 1100000004~201803~400000066    column=d:employees, timestamp=1530011497018, value=2
 1100000004~201803~400000066    column=d:entref, timestamp=1530011497003, value=9900000242
 1100000004~201803~400000066    column=d:ern, timestamp=1530011497001, value=4000000011
 1100000004~201803~400000066    column=d:luref, timestamp=1530011496999, value=100000508723
 1100000004~201803~400000066    column=d:lurn, timestamp=1530011496998, value=400000066
 1100000004~201803~400000066    column=d:name, timestamp=1530011497005, value=2-ND LU OF IBM LTD
 1100000004~201803~400000066    column=d:postcode, timestamp=1530011497015, value=NP10 6XG
 1100000004~201803~400000066    column=d:sic07, timestamp=1530011497017, value=2233
 1100000004~201803~400000066    column=d:trading_style, timestamp=1530011497006, value=A

 1100000004~201803~400000077    column=d:address1, timestamp=1530011496974, value=BSTER DEPT
 1100000004~201803~400000077    column=d:address2, timestamp=1530011496976, value=MAILPOINT A1F
 1100000004~201803~400000077    column=d:address3, timestamp=1530011496983, value=P O BOX 41
 1100000004~201803~400000077    column=d:address4, timestamp=1530011496988, value=NORTH HARBOUR
 1100000004~201803~400000077    column=d:address5, timestamp=1530011496990, value=PORTSMOUTH
 1100000004~201803~400000077    column=d:employees, timestamp=1530011496996, value=2
 1100000004~201803~400000077    column=d:entref, timestamp=1530011496969, value=9900000242
 1100000004~201803~400000077    column=d:ern, timestamp=1530011496967, value=4000000011
 1100000004~201803~400000077    column=d:luref, timestamp=1530011496965, value=100000459235
 1100000004~201803~400000077    column=d:lurn, timestamp=1530011496964, value=400000077
 1100000004~201803~400000077    column=d:name, timestamp=1530011496971, value=IBM LTD
 1100000004~201803~400000077    column=d:postcode, timestamp=1530011496992, value=PO6 3AU
 1100000004~201803~400000077    column=d:sic07, timestamp=1530011496994, value=34567
 1100000004~201803~400000077    column=d:trading_style, timestamp=1530011496973, value=C          */