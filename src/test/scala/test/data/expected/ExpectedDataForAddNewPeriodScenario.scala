package test.data.expected

import test.data.TestIds
import model.domain.{Enterprise, HFileRow, KVCell, LocalUnit}

trait ExpectedDataForAddNewPeriodScenario extends TestIds{

  val newPeriodLinks = List(
    HFileRow("00012345~CH~201804",List(KVCell("p_LEU","100000508724"))),
    HFileRow("00032262~CH~201804",List(KVCell("p_LEU","100000246017"))),
    HFileRow("01113199~CH~201804",List(KVCell("p_LEU","100000508723"))),
    HFileRow("04186804~CH~201804",List(KVCell("p_LEU","100000827984"))),
    HFileRow("04223160~CH~201804",List(KVCell("p_LEU","100000459235"))),
    HFileRow("100000246017~LEU~201804",List(KVCell("c_00032262","CH"), KVCell("c_111222333","VAT"), KVCell("c_1152L","PAYE"), KVCell("c_1153L","PAYE"), KVCell("p_ENT","3000000011")).sortBy(_.column)),

    HFileRow("100000459235~LEU~201804",List(KVCell("c_04223160","CH"), KVCell("c_1166L","PAYE"), KVCell("c_1177L","PAYE"), KVCell("c_222666000","VAT"), KVCell("c_555666777","VAT"), KVCell("p_ENT","4000000011")).sortBy(_.column)),
    //CH c_01113199 changed from c_04223165
    HFileRow("100000508723~LEU~201804",List(KVCell("c_01113199","CH"), KVCell("c_111000111","VAT"), KVCell("c_1188L","PAYE"), KVCell("c_1199L","PAYE"), KVCell("p_ENT","4000000011")).sortBy(_.column)),
    //PAYE ref 3333L added with update
    HFileRow("100000508724~LEU~201804",List(KVCell("c_00012345","CH"), KVCell("c_3333L","PAYE"), KVCell("c_5555L","PAYE"), KVCell("c_999888777","VAT"), KVCell("p_ENT","4000000011")).sortBy(_.column)),
    //CH 04186804 changed from 00032263
    HFileRow("100000827984~LEU~201804",List(KVCell("c_04186804","CH"), KVCell("c_1154L","PAYE"), KVCell("c_1155L","PAYE"), KVCell("c_222333444","VAT"), KVCell("p_ENT","3000000011")).sortBy(_.column)),
    //c_00032261 CH removed
    HFileRow("100002826247~LEU~201804",List(KVCell("c_1151L","PAYE"),  KVCell("c_123123123","VAT"), KVCell("p_ENT",entWithMissingLouId)).sortBy(_.column)),
    HFileRow("111000111~VAT~201804",List(KVCell("p_LEU","100000508723"))),
    HFileRow("111222333~VAT~201804",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1151L~PAYE~201804",List(KVCell("p_LEU","100002826247"))),
    HFileRow("1152L~PAYE~201804",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1153L~PAYE~201804",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1154L~PAYE~201804",List(KVCell("p_LEU","100000827984"))),
    HFileRow("1155L~PAYE~201804",List(KVCell("p_LEU","100000827984"))),
    HFileRow("1166L~PAYE~201804",List(KVCell("p_LEU","100000459235"))),
    HFileRow("1177L~PAYE~201804",List(KVCell("p_LEU","100000459235"))),
    HFileRow("1188L~PAYE~201804",List(KVCell("p_LEU","100000508723"))),
    HFileRow("1199L~PAYE~201804",List(KVCell("p_LEU","100000508723"))),
    HFileRow("123123123~VAT~201804",List(KVCell("p_LEU","100002826247"))),
    HFileRow(s"$entWithMissingLouId~ENT~201804",List(KVCell("c_100002826247","LEU"), KVCell(s"c_$missingLouLurn","LOU")).sortBy(_.column)),
    HFileRow(s"$missingLouLurn~LOU~201804",List(KVCell("p_ENT",entWithMissingLouId))),
    HFileRow("222333444~VAT~201804",List(KVCell("p_LEU","100000827984"))),
    HFileRow("222666000~VAT~201804",List(KVCell("p_LEU","100000459235"))),
    HFileRow("3000000011~ENT~201804",List(KVCell("c_100000246017","LEU"), KVCell("c_100000827984","LEU"), KVCell("c_300000088","LOU"), KVCell("c_300000099","LOU")).sortBy(_.column)),
    HFileRow("300000055~LOU~201804",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000066~LOU~201804",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000077~LOU~201804",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000088~LOU~201804",List(KVCell("p_ENT","3000000011"))),
    HFileRow("300000099~LOU~201804",List(KVCell("p_ENT","3000000011"))),
    HFileRow("33322444~CH~201804",List(KVCell("p_LEU","999000508999"))),   //new CH, added with update
    HFileRow("3333L~PAYE~201804",List(KVCell("p_LEU","100000508724"))),    //new PAYE, added with update, see line 21
    HFileRow("4000000011~ENT~201804",List(KVCell("c_100000459235","LEU"), KVCell("c_100000508723","LEU"), KVCell("c_100000508724","LEU"), KVCell("c_300000055","LOU"), KVCell("c_300000066","LOU"), KVCell("c_300000077","LOU")).sortBy(_.column)),
    HFileRow(s"$newEntErn~ENT~201804",List(KVCell(s"c_$newLouLurn","LOU"), KVCell("c_999000508999","LEU"))), //new enterprise
    HFileRow(s"$newLouLurn~LOU~201804",List(KVCell("p_ENT",newEntErn))),
    HFileRow("5555L~PAYE~201804",List(KVCell("p_LEU","100000508724"))),
    HFileRow("555666777~VAT~201804",List(KVCell("p_LEU","100000459235"))),
    HFileRow("919100010~VAT~201804",List(KVCell("p_LEU","999000508999"))), //new, added with new LEU(and hence ENT and LOU)
    /// CH c_33322444 added with update, see line 48
    HFileRow("999000508999~LEU~201804",List(KVCell("c_33322444","CH"), KVCell("c_919100010","VAT"), KVCell("p_ENT",newEntErn)).sortBy(_.column)), // new LEU
    HFileRow("999888777~VAT~201804",List(KVCell("p_LEU","100000508724")))
  )

  val newPeriodEnts = List(
    Enterprise("4000000011",Some("9900000242"),"IBM LTD","BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("NORTH HARBOUR"),"PO6 3AU",Some("C"),"34567","1",Some("4"),Some("8"),None,Some("180"),Some("180"),None,None),
    Enterprise("3000000011",Some("9900000126"),"BLACKWELLGROUP LTD","GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX",Some("B"),"23456","1",Some("17"),Some("20"),None,Some("1175"),Some("585"),Some("590"),None),
    Enterprise(entWithMissingLouId,Some("9900000009"),"INDUSTRIES LTD","P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("REDDITCH"),"B22 2TL",Some("A"),"12345","1",Some("2"),Some("4"),None,None,None,None,None),
    Enterprise(newEntErn,Some("9999999999"),"NEW ENTERPRISE LU","",None,None,None,None,"W1A 1AA",None,"10001","9",None,None,None,Some("85"),Some("85"),None,None)
  )

  val newPeriodLocalUnits = List(
    LocalUnit("200000099",Some("100002826247"),entWithMissingLouId,Some("9900000009"),"INDUSTRIES LTD",None,"P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("WORCESTERSHIRE"),"B22 2TL","12345","2"),
    LocalUnit("300000088",Some("100000827984"),"3000000011",Some("9900000126"),"2-ND LU OF BLACKWELLGROUP LTD",None,"North End Rd lane",Some("Croydon"),Some("Surrey"),None,None,"CR0 1AA","1122","2"),
    LocalUnit("300000099",Some("100000246017"),"3000000011",Some("9900000126"),"BLACKWELLGROUP LTD",None,"GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX","23456","2"),
    LocalUnit("400000055",Some("100000508724"),"4000000011",Some("9900000242"),"3-RD LU OF IBM LTD",None,"IBM HOUSE",Some("Smile Street"),Some("Cardiff"),Some("SOUTH WALES"),None,"CF23 9EU","3344","1"),
    LocalUnit("400000066",Some("100000508723"),"4000000011",Some("9900000242"),"2-ND LU OF IBM LTD",None,"IT DEPT",Some("1 Hight Street"),Some("Newport"),Some("SOUTH WALES"),None,"NP10 6XG","2233","2"),
    LocalUnit("400000077",Some("100000459235"),"4000000011",Some("9900000242"),"IBM LTD",None,"BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"PO6 3AU","34567","2"),
    LocalUnit(newLouLurn, None, newEntErn, None, "NEW ENTERPRISE LU", None, "", None, None, None, None, "W1A 1AA", "10001", "0")
  )

  val newPeriodWithMissingLocalUnit = List(
    LocalUnit(s"$missingLouLurn",None, entWithMissingLouId,Some("9900000009"),"INDUSTRIES LTD",None,"P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("WORCESTERSHIRE"),"B22 2TL","12345","2"),
    LocalUnit("300000088",Some("100000827984"),"3000000011",Some("9900000126"),"2-ND LU OF BLACKWELLGROUP LTD",None,"North End Rd lane",Some("Croydon"),Some("Surrey"),None,None,"CR0 1AA","1122","2"),
    LocalUnit("300000099",Some("100000246017"),"3000000011",Some("9900000126"),"BLACKWELLGROUP LTD",None,"GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX","23456","2"),
    LocalUnit("400000055",Some("100000508724"),"4000000011",Some("9900000242"),"3-RD LU OF IBM LTD",None,"IBM HOUSE",Some("Smile Street"),Some("Cardiff"),Some("SOUTH WALES"),None,"CF23 9EU","3344","1"),
    LocalUnit("400000066",Some("100000508723"),"4000000011",Some("9900000242"),"2-ND LU OF IBM LTD",None,"IT DEPT",Some("1 Hight Street"),Some("Newport"),Some("SOUTH WALES"),None,"NP10 6XG","2233","2"),
    LocalUnit("400000077",Some("100000459235"),"4000000011",Some("9900000242"),"IBM LTD",None,"BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"PO6 3AU","34567","2"),
    LocalUnit(newLouLurn, None, newEntErn, None, "NEW ENTERPRISE LU", None, "", None, None, None, None, "W1A 1AA", "10001", "0")
  )

}
