package utils.data.expected

import model.domain._
import utils.data.TestIds

trait ExpectedDataForAddNewPeriodScenario extends TestIds{


  val expectedNewPeriodLinks = List[LinkRecord](

  LinkRecord("111111111-TEST-ERN",List(ReportingUnitLink("11111111-TEST-RURN",List("11111111-TEST-LURN"),"111111111-TEST-ERN")),List(LegalUnitLink("999000508999",Some("33322444"),List("1999Z"),List("919100010000")))),
  LinkRecord(entWithMissingLouId,List(ReportingUnitLink("MISS-LOU-TEST-RURN",List("MISS-LOU-TEST-LURN"),"2000000011")),List(LegalUnitLink("100002826247",None,List("1151L"),List("123123123000")))),
  LinkRecord("3000000011",List(ReportingUnitLink("2000000002",List("300000088", "300000099"),"3000000011")),List(LegalUnitLink("100000246017",Some("00032262"),List("1152L", "1153L"),List("111222333000")), LegalUnitLink("100000827984",Some("04186804"),List("1154L", "1155L"),List("111222333001")))),
  LinkRecord("4000000011",List(ReportingUnitLink("5000000005",List("400000055", "400000066", "400000077"),"4000000011")),List(LegalUnitLink("100000459235",Some("04223160"),List("1166L", "1177L"),List("555666777000", "555666777001")), LegalUnitLink("100000508723",Some("01113199"),List("1188L", "1199L"),List("555666777002")), LegalUnitLink("100000508724",Some("00012345"),List("3333L", "5555L"),List("999888777000")))),
  LinkRecord("5000000011",List(ReportingUnitLink("6000000006",List("550000088"),"5000000011")),List(LegalUnitLink("100000601835",Some("01012444"),List("9876L"),List("555666777003"))))
  )
  

  val newPeriodLinks = List(
    HFileRow("CH~00012345",List(KVCell("p_LEU","100000508724"))),
    HFileRow("CH~00032262",List(KVCell("p_LEU","100000246017"))),
    HFileRow("CH~01113199",List(KVCell("p_LEU","100000508723"))),
    HFileRow("CH~04186804",List(KVCell("p_LEU","100000827984"))),
    HFileRow("CH~04223160",List(KVCell("p_LEU","100000459235"))),
    HFileRow("LEU~100000246017",List(KVCell("c_00032262","CH"), KVCell("c_111222333","VAT"), KVCell("c_1152L","PAYE"), KVCell("c_1153L","PAYE"), KVCell("p_ENT","3000000011")).sortBy(_.column)),

    HFileRow("LEU~100000459235",List(KVCell("c_04223160","CH"), KVCell("c_1166L","PAYE"), KVCell("c_1177L","PAYE"), KVCell("c_222666000","VAT"), KVCell("c_555666777","VAT"), KVCell("p_ENT","4000000011")).sortBy(_.column)),
    //CH c_01113199 changed from c_04223165
    HFileRow("LEU~100000508723",List(KVCell("c_01113199","CH"), KVCell("c_111000111","VAT"), KVCell("c_1188L","PAYE"), KVCell("c_1199L","PAYE"), KVCell("p_ENT","4000000011")).sortBy(_.column)),
    //PAYE ref 3333L added with update
    HFileRow("LEU~100000508724",List(KVCell("c_00012345","CH"), KVCell("c_3333L","PAYE"), KVCell("c_5555L","PAYE"), KVCell("c_999888777","VAT"), KVCell("p_ENT","4000000011")).sortBy(_.column)),
    //CH 04186804 changed from 00032263
    HFileRow("LEU~100000827984",List(KVCell("c_04186804","CH"), KVCell("c_1154L","PAYE"), KVCell("c_1155L","PAYE"), KVCell("c_222333444","VAT"), KVCell("p_ENT","3000000011")).sortBy(_.column)),
    //c_00032261 CH removed
    HFileRow("LEU~100002826247",List(KVCell("c_1151L","PAYE"),  KVCell("c_123123123","VAT"), KVCell("p_ENT",entWithMissingLouId)).sortBy(_.column)),
    HFileRow("VAT~111000111",List(KVCell("p_LEU","100000508723"))),
    HFileRow("VAT~111222333",List(KVCell("p_LEU","100000246017"))),
    HFileRow("PAYE~1151L",List(KVCell("p_LEU","100002826247"))),
    HFileRow("PAYE~1152L",List(KVCell("p_LEU","100000246017"))),
    HFileRow("PAYE~1153L",List(KVCell("p_LEU","100000246017"))),
    HFileRow("PAYE~1154L",List(KVCell("p_LEU","100000827984"))),
    HFileRow("PAYE~1155L",List(KVCell("p_LEU","100000827984"))),
    HFileRow("PAYE~1166L",List(KVCell("p_LEU","100000459235"))),
    HFileRow("PAYE~1177L",List(KVCell("p_LEU","100000459235"))),
    HFileRow("PAYE~1188L",List(KVCell("p_LEU","100000508723"))),
    HFileRow("PAYE~1199L",List(KVCell("p_LEU","100000508723"))),
    HFileRow("PAYE~1999Z",List(KVCell("p_LEU","999000508999"))), //new, added with new LEU(and hence ENT and LOU)
    HFileRow("VAT~123123123",List(KVCell("p_LEU","100002826247"))),
    HFileRow(s"ENT~$entWithMissingLouId",List(KVCell("c_100002826247","LEU"), KVCell(s"c_$missingLouLurn","LOU")).sortBy(_.column)),
    HFileRow(s"LOU~$missingLouLurn",List(KVCell("p_ENT",entWithMissingLouId))),
    HFileRow("VAT~222333444",List(KVCell("p_LEU","100000827984"))),
    HFileRow("VAT~222666000",List(KVCell("p_LEU","100000459235"))),
    HFileRow("ENT~3000000011",List(KVCell("c_100000246017","LEU"), KVCell("c_100000827984","LEU"), KVCell("c_300000088","LOU"), KVCell("c_300000099","LOU")).sortBy(_.column)),
    HFileRow("LOU~300000055",List(KVCell("p_ENT","4000000011"))),
    HFileRow("LOU~300000066",List(KVCell("p_ENT","4000000011"))),
    HFileRow("LOU~300000077",List(KVCell("p_ENT","4000000011"))),
    HFileRow("LOU~300000088",List(KVCell("p_ENT","3000000011"))),
    HFileRow("LOU~300000099",List(KVCell("p_ENT","3000000011"))),
    HFileRow("CH~33322444",List(KVCell("p_LEU","999000508999"))),   //new CH, added with update
    HFileRow("PAYE~3333L",List(KVCell("p_LEU","100000508724"))),    //new PAYE, added with update, see line 21
    HFileRow("ENT~4000000011",List(KVCell("c_100000459235","LEU"), KVCell("c_100000508723","LEU"), KVCell("c_100000508724","LEU"), KVCell("c_300000055","LOU"), KVCell("c_300000066","LOU"), KVCell("c_300000077","LOU")).sortBy(_.column)),
    HFileRow(s"ENT~$newEntErn",List(KVCell(s"c_$newLouLurn","LOU"), KVCell("c_999000508999","LEU"))), //new enterprise
    HFileRow(s"LOU~$newLouLurn",List(KVCell("p_ENT",newEntErn),KVCell("p_ENT",newEntErn))),
    HFileRow("PAYE~5555L",List(KVCell("p_LEU","100000508724"))),
    HFileRow("VAT~555666777",List(KVCell("p_LEU","100000459235"))),
    HFileRow("VAT~919100010",List(KVCell("p_LEU","999000508999"))), //new, added with new LEU(and hence ENT and LOU)
    /// CH c_33322444 added with update, see line 48
    HFileRow("LEU~999000508999",List(KVCell("c_33322444","CH"), KVCell("c_919100010","VAT"), KVCell("c_1999Z","PAY"), KVCell("p_ENT",newEntErn)).sortBy(_.column)), // new LEU
    HFileRow("VAT~999888777",List(KVCell("p_LEU","100000508724")))
  )

  val newPeriodEnts = List(
    Enterprise(newEntErn,newRuPrn,None,"NEW ENTERPRISE LU","",None,None,None,None,"W1A 1AA",None,"10001","9",Some("3"),Some("5"),None,Some("85"),Some("85"),None,None),
    Enterprise("2000000011",entPrnWithMissingLouId,Some("9900000009"),"INDUSTRIES LTD","P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("WORCESTERSHIRE"),"B22 2TL",Some("A"),"12345","1",Some("2"),Some("4"),None,Some("390"),None,Some("390"),None),
    Enterprise("3000000011","0.311",Some("9900000126"),"BLACKWELLGROUP LTD","GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX",Some("B"),"23456","1",Some("19"),Some("20"),None,Some("585"),Some("585"),None,None),
    Enterprise("4000000011","0.411",Some("9900000242"),"IBM LTD","BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"PO6 3AU",Some("C"),"34567","1",Some("4"),Some("8"),Some("444"),Some("704"),None,Some("260"),Some("1000")),
    Enterprise("5000000011","0.511",Some("9900000777"),"MBI LTD","99 Pen-Y-Lan Terrace",Some("Unit 11"),Some("Cardiff"),None,None,"CF23 9EU",Some("U"),"44044","5",Some("5"),Some("5"),Some("555"),Some("555"),None,None,Some("1000"))
  )

  val newPeriodEntsWithoutCalculations = List[Enterprise](
    /*Enterprise("4000000011",Some("9900000242"),"IBM LTD","BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"PO6 3AU",Some("C"),"34567","1",None,None,None,None,None,None,None),
    Enterprise("3000000011",Some("9900000126"),"BLACKWELLGROUP LTD","GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX",Some("B"),"23456","1",None,None,None,None,None,None,None),
    Enterprise(entWithMissingLouId,Some("9900000009"),"INDUSTRIES LTD","P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("WORCESTERSHIRE"),"B22 2TL",Some("A"),"12345","1",None,None,None,None,None,None,None),
    Enterprise(newEntErn_2,None,"NEW ENTERPRISE LU","",None,None,None,None,"W1A 1AA",None,"10001","9",None,None,None,None,None,None,None),
    Enterprise("5000000011",Some("9900000777"),"MBI LTD","99 Pen-Y-Lan Terrace",Some("Unit 11"),Some("Cardiff"),None,None,"CF23 9EU",Some("U"),"44044","5",None,None,None,None,None,None,None)*/
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
    LegalUnit("100000601835","5000000011",Some("00032262"),"MBI LTD",Some("U"),"99 Pen-Y-Lan Terrace",Some("Unit 11"),Some("Cardiff"),None,None,"CF23 9EU","44044",Some("4"),None,"2",Some("C"),"06/08/2010",None,None,None),
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
