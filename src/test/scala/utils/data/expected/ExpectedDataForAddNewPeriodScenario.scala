package utils.data.expected

import model.domain._
import utils.data.TestIds

trait ExpectedDataForAddNewPeriodScenario extends TestIds{
  
  

  val newPeriodLinks = List(
    HFileRow("00012345~CH",List(KVCell("p_LEU","100000508724"))),
    HFileRow("00032262~CH",List(KVCell("p_LEU","100000246017"))),
    HFileRow("01113199~CH",List(KVCell("p_LEU","100000508723"))),
    HFileRow("04186804~CH",List(KVCell("p_LEU","100000827984"))),
    HFileRow("04223160~CH",List(KVCell("p_LEU","100000459235"))),
    HFileRow("100000246017~LEU",List(KVCell("c_00032262","CH"), KVCell("c_111222333","VAT"), KVCell("c_1152L","PAYE"), KVCell("c_1153L","PAYE"), KVCell("p_ENT","3000000011")).sortBy(_.column)),

    HFileRow("100000459235~LEU",List(KVCell("c_04223160","CH"), KVCell("c_1166L","PAYE"), KVCell("c_1177L","PAYE"), KVCell("c_222666000","VAT"), KVCell("c_555666777","VAT"), KVCell("p_ENT","4000000011")).sortBy(_.column)),
    //CH c_01113199 changed from c_04223165
    HFileRow("100000508723~LEU",List(KVCell("c_01113199","CH"), KVCell("c_111000111","VAT"), KVCell("c_1188L","PAYE"), KVCell("c_1199L","PAYE"), KVCell("p_ENT","4000000011")).sortBy(_.column)),
    //PAYE ref 3333L added with update
    HFileRow("100000508724~LEU",List(KVCell("c_00012345","CH"), KVCell("c_3333L","PAYE"), KVCell("c_5555L","PAYE"), KVCell("c_999888777","VAT"), KVCell("p_ENT","4000000011")).sortBy(_.column)),
    //CH 04186804 changed from 00032263
    HFileRow("100000827984~LEU",List(KVCell("c_04186804","CH"), KVCell("c_1154L","PAYE"), KVCell("c_1155L","PAYE"), KVCell("c_222333444","VAT"), KVCell("p_ENT","3000000011")).sortBy(_.column)),
    //c_00032261 CH removed
    HFileRow("100002826247~LEU",List(KVCell("c_1151L","PAYE"),  KVCell("c_123123123","VAT"), KVCell("p_ENT",entWithMissingLouId)).sortBy(_.column)),
    HFileRow("111000111~VAT",List(KVCell("p_LEU","100000508723"))),
    HFileRow("111222333~VAT",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1151L~PAYE",List(KVCell("p_LEU","100002826247"))),
    HFileRow("1152L~PAYE",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1153L~PAYE",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1154L~PAYE",List(KVCell("p_LEU","100000827984"))),
    HFileRow("1155L~PAYE",List(KVCell("p_LEU","100000827984"))),
    HFileRow("1166L~PAYE",List(KVCell("p_LEU","100000459235"))),
    HFileRow("1177L~PAYE",List(KVCell("p_LEU","100000459235"))),
    HFileRow("1188L~PAYE",List(KVCell("p_LEU","100000508723"))),
    HFileRow("1199L~PAYE",List(KVCell("p_LEU","100000508723"))),
    HFileRow("1999Z~PAYE",List(KVCell("p_LEU","999000508999"))), //new, added with new LEU(and hence ENT and LOU)
    HFileRow("123123123~VAT",List(KVCell("p_LEU","100002826247"))),
    HFileRow(s"$entWithMissingLouId~ENT",List(KVCell("c_100002826247","LEU"), KVCell(s"c_$missingLouLurn","LOU")).sortBy(_.column)),
    HFileRow(s"$missingLouLurn~LOU",List(KVCell("p_ENT",entWithMissingLouId))),
    HFileRow("222333444~VAT",List(KVCell("p_LEU","100000827984"))),
    HFileRow("222666000~VAT",List(KVCell("p_LEU","100000459235"))),
    HFileRow("3000000011~ENT",List(KVCell("c_100000246017","LEU"), KVCell("c_100000827984","LEU"), KVCell("c_300000088","LOU"), KVCell("c_300000099","LOU")).sortBy(_.column)),
    HFileRow("300000055~LOU",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000066~LOU",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000077~LOU",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000088~LOU",List(KVCell("p_ENT","3000000011"))),
    HFileRow("300000099~LOU",List(KVCell("p_ENT","3000000011"))),
    HFileRow("33322444~CH",List(KVCell("p_LEU","999000508999"))),   //new CH, added with update
    HFileRow("3333L~PAYE",List(KVCell("p_LEU","100000508724"))),    //new PAYE, added with update, see line 21
    HFileRow("4000000011~ENT",List(KVCell("c_100000459235","LEU"), KVCell("c_100000508723","LEU"), KVCell("c_100000508724","LEU"), KVCell("c_300000055","LOU"), KVCell("c_300000066","LOU"), KVCell("c_300000077","LOU")).sortBy(_.column)),
    HFileRow(s"$newEntErn~ENT",List(KVCell(s"c_$newLouLurn","LOU"), KVCell("c_999000508999","LEU"))), //new enterprise
    HFileRow(s"$newLouLurn~LOU",List(KVCell("p_ENT",newEntErn),KVCell("p_ENT",newEntErn))),
    HFileRow("5555L~PAYE",List(KVCell("p_LEU","100000508724"))),
    HFileRow("555666777~VAT",List(KVCell("p_LEU","100000459235"))),
    HFileRow("919100010~VAT",List(KVCell("p_LEU","999000508999"))), //new, added with new LEU(and hence ENT and LOU)
    /// CH c_33322444 added with update, see line 48
    HFileRow("999000508999~LEU",List(KVCell("c_33322444","CH"), KVCell("c_919100010","VAT"), KVCell("c_1999Z","PAY"), KVCell("p_ENT",newEntErn)).sortBy(_.column)), // new LEU
    HFileRow("999888777~VAT",List(KVCell("p_LEU","100000508724")))
  )

  val newPeriodEnts = List(
    Enterprise("111111111-TEST-ERN",None,"NEW ENTERPRISE LU","",None,None,None,None,"W1A 1AA",None,"10001","9",Some("3"),Some("5"),None,Some("85"),Some("85"),None,None),
    Enterprise("2000000011",Some("9900000009"),"INDUSTRIES LTD","P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("WORCESTERSHIRE"),"B22 2TL",Some("A"),"12345","1",Some("2"),Some("4"),None,Some("390"),None,Some("390"),None),
    Enterprise("3000000011",Some("9900000126"),"BLACKWELLGROUP LTD","GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX",Some("B"),"23456","1",Some("19"),Some("20"),None,Some("585"),Some("585"),None,None),
    Enterprise("4000000011",Some("9900000242"),"IBM LTD","BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"PO6 3AU",Some("C"),"34567","1",Some("4"),Some("8"),Some("444"),Some("704"),None,Some("260"),Some("1000")),
    Enterprise("5000000011",Some("9900000777"),"MBI LTD","99 Pen-Y-Lan Terrace",Some("Unit 11"),Some("Cardiff"),None,None,"CF23 9EU",Some("U"),"44044","5",Some("5"),Some("5"),Some("555"),Some("555"),None,None,Some("1000"))
  )

  val newPeriodEntsWithoutCalculations = List(
    Enterprise("4000000011",Some("9900000242"),"IBM LTD","BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"PO6 3AU",Some("C"),"34567","1",None,None,None,None,None,None,None),
    Enterprise("3000000011",Some("9900000126"),"BLACKWELLGROUP LTD","GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX",Some("B"),"23456","1",None,None,None,None,None,None,None),
    Enterprise(entWithMissingLouId,Some("9900000009"),"INDUSTRIES LTD","P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("WORCESTERSHIRE"),"B22 2TL",Some("A"),"12345","1",None,None,None,None,None,None,None),
    Enterprise(newEntErn_2,None,"NEW ENTERPRISE LU","",None,None,None,None,"W1A 1AA",None,"10001","9",None,None,None,None,None,None,None),
    Enterprise("5000000011",Some("9900000777"),"MBI LTD","99 Pen-Y-Lan Terrace",Some("Unit 11"),Some("Cardiff"),None,None,"CF23 9EU",Some("U"),"44044","5",None,None,None,None,None,None,None)
  )

  val newPeriodLocalUnits = List(
    LocalUnit(missingLouLurn,Some("100002826247"),entWithMissingLouId,missingLouRurn,Some(missingLouRuref),"INDUSTRIES LTD",Some("6600000006"),Some("A"),"P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("WORCESTERSHIRE"),"B22 2TL","12345","2"),
    LocalUnit("300000088",Some("100000827984"),"3000000011","2000000002",Some("9900000126"),"2-ND LU OF BLACKWELLGROUP LTD",Some("9900000999"),Some("B"),"North End Rd lane",Some("Croydon"),Some("Surrey"),None,None,"CR0 1AA","1122","2"),
    LocalUnit("300000099",Some("100000246017"),"3000000011","2000000002",Some("9900000126"),"BLACKWELLGROUP LTD",Some("9900000999"),Some("B"),"GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX","23456","2"),
    LocalUnit("400000055",Some("100000508724"),"4000000011","5000000005", Some("9900000242"),"3-RD LU OF IBM LTD",Some("8800000888"),Some("B"),"IBM HOUSE",Some("Smile Street"),Some("Cardiff"),Some("SOUTH WALES"),None,"CF23 9EU","3344","1"),
    LocalUnit("400000066",Some("100000508723"),"4000000011","5000000005",Some("9900000242"),"2-ND LU OF IBM LTD",Some("8800000888"),Some("A"),"IT DEPT",Some("1 Hight Street"),Some("Newport"),Some("SOUTH WALES"),None,"NP10 6XG","2233","2"),
    LocalUnit("400000077",Some("100000459235"),"4000000011","5000000005", Some("9900000242"),"IBM LTD",Some("8800000888"),Some("C"),"BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"PO6 3AU","34567","2"),
    LocalUnit("550000088",Some("100000601835"),"5000000011","6000000006", Some("9900000777"),"MBI LTD",Some("7700000777"),Some("U"),"99 Pen-Y-Lan Terrace",Some("Unit 11"),Some("Cardiff"),None,None,"CF23 9EU","44044","5"),
    LocalUnit(newLouLurn, None, newEntErn, newRuRurn,None,"NEW ENTERPRISE LU",None,  None, "", None, None, None, None, "W1A 1AA", "10001", "3")
  )

  val newPeriodReportingUnits = List[ReportingUnit](
    ReportingUnit(missingLouRurn,None,entWithMissingLouId,Some("9900000009"),"1111111111","INDUSTRIES LTD",Some("A"), "1","P O BOX 22",Some("INDUSTRIES HOUSE"), Some("WHITE LANE"),Some("REDDITCH"), Some("WORCESTERSHIRE"), "B22 2TL","12345","2","1","300"),
    ReportingUnit("2000000002",None,"3000000011",Some("9900000126"),"3333333333","BLACKWELLGROUP LTD",Some("B"), "1","GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX","23456","4","3","500"),
    ReportingUnit("5000000005",None,"4000000011",Some("9900000242"),"6666666666","IBM LTD",Some("C"), "1","BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"PO6 3AU","34567","7","6","800"),
    ReportingUnit("6000000006",None,"5000000011",Some("9900000777"),"7777777777","MBI LTD",Some("U"), "5","99 Pen-Y-Lan Terrace",Some("Unit 11"),Some("Cardiff"),None,None,"CF23 9EU","44044","8","7","900"),
    ReportingUnit("11111111-TEST-RURN",None,"111111111-TEST-ERN",None,newRuPrn,"NEW ENTERPRISE LU",None,"9","",None,None,None,None,"W1A 1AA","10001","3","","")

  )

  val newPeriodLegalUnits = List[LegalUnit](
    LegalUnit("100000246017","3000000011",Some("00032262"),"BLACKWELLGROUP LTD",Some("B"),"GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX","23456",Some("2"),Some("200"),"1",Some("A"),"02/042017",None,None,None),
    LegalUnit("100000459235","4000000011",Some("04223164"),"IBM LTD",Some("C"),"BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"CF23 9EU","3344",Some("2"),None,"1",Some("A"),"04/06/2015",None,None,None),
    LegalUnit("100000508723","4000000011",Some("04223165"),"2-ND LU OF IBM LTD",Some("A"),"IT DEPT",Some("1 Hight Street"),Some("Newport"),Some("SOUTH WALES"),None,"NP10 6XG","2233",Some("2"),None,"1",Some("B"),"05/07/2014",None,None,None),
    LegalUnit("100000508724","4000000011",Some("00012345"),"3-RD LU OF IBM LTD",Some("B"),"IBM HOUSE",Some("Smile Street"),Some("Cardiff"),Some("SOUTH WALES"),None,"CF23 9EU","3344",Some("1"),None,"1",Some("C"),"06/08/2013",None,None,None),
    LegalUnit("100000508888","5000000011",Some("00032262"),"MBI LTD",Some("U"),"99 Pen-Y-Lan Terrace",Some("Unit 11"),Some("Cardiff"),None,None,"CF23 9EU","44044",Some("4"),None,"2",Some("C"),"06/08/2010",None,None,None),
    LegalUnit("100000827984","3000000011",Some("100000827984"),"2-ND LU OF BLACKWELLGROUP LTD",Some("B"),"North End Rd lane",Some("Croydon"),Some("Surrey"),None,None,"CR0 1AA","1122",Some("2"),None,"1",Some("C"),"03/05/2016",None,None,None),
    LegalUnit("100002826247","2000000011",Some("00032262"),"INDUSTRIES LTD",Some("A"),"P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("WORCESTERSHIRE"),"B22 2TL","12345",Some("4"),Some("500"),"1",Some("B"),"01/03/2018",None,None,None),
    LegalUnit("999000508999","111111111-TEST-ERN",Some("33322444"),"NEW ENTERPRISE LU",None,"",None,None,None,None,"W1A 1AA","10001",Some("5"),Some("X"),"9",Some("W"),"",None,None,Some("123456"))
      )

  val newPeriodLocalUnitsWithoutCalculations = newPeriodLocalUnits/*List(
    LocalUnit(missingLouLurn,Some("100002826247"),entWithMissingLouId,Some("9900000009"),"INDUSTRIES LTD",None,"P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("WORCESTERSHIRE"),"B22 2TL","12345","2"),
    LocalUnit("300000088",Some("100000827984"),"3000000011",Some("9900000126"),"2-ND LU OF BLACKWELLGROUP LTD",None,"North End Rd lane",Some("Croydon"),Some("Surrey"),None,None,"CR0 1AA","1122","2"),
    LocalUnit("300000099",Some("100000246017"),"3000000011",Some("9900000126"),"BLACKWELLGROUP LTD",None,"GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX","23456","2"),
    LocalUnit("400000055",Some("100000508724"),"4000000011",Some("9900000242"),"3-RD LU OF IBM LTD",None,"IBM HOUSE",Some("Smile Street"),Some("Cardiff"),Some("SOUTH WALES"),None,"CF23 9EU","3344","1"),
    LocalUnit("400000066",Some("100000508723"),"4000000011",Some("9900000242"),"2-ND LU OF IBM LTD",None,"IT DEPT",Some("1 Hight Street"),Some("Newport"),Some("SOUTH WALES"),None,"NP10 6XG","2233","2"),
    LocalUnit("400000077",Some("100000459235"),"4000000011",Some("9900000242"),"IBM LTD",None,"BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"PO6 3AU","34567","2"),
    LocalUnit("550000088",Some("100000601835"),"5000000011",Some("9900000777"),"MBI LTD",None,"99 Pen-Y-Lan Terrace",Some("Unit 11"),Some("Cardiff"),None,None,"CF23 9EU","44044","5"),
    LocalUnit(newLouLurn_2, None, newEntErn_2, None, "NEW ENTERPRISE LU", None, "", None, None, None, None, "W1A 1AA", "10001", "")
  )*/

  val newPeriodWithMissingLocalUnit = newPeriodLocalUnits.tail
}
