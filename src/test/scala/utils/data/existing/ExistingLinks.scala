package utils.data.existing

import model.domain.{HFileRow, KVCell}
import utils.data.TestIds


trait ExistingLinks {this:TestIds =>

//     HFileRow("00032261~CH",List(KVCell("p_LEU","100002826247"))),
//{"id":100000601835, "BusinessName": "MBI LTD", "UPRN": 789789, "PostCode": "CF23 9EU", "IndustryCode": "44044", "LegalStatus": "5", "TradingStatus": "X", "Turnover": "Y", "EmploymentBands": "Z", "VatRefs": [555666777003],"PayeRefs": ["9876L"], "CompanyNo": "01012444","ern":"5000000011"}
  val existingLinksForAddNewPeriodScenarion = List(
    HFileRow("CH~00012345",List(KVCell("p_LEU","100000508724"))),
    HFileRow("CH~00032262",List(KVCell("p_LEU","100000246017"))),
    HFileRow("CH~04223165",List(KVCell("p_LEU","100000508723"))),
    HFileRow("CH~00032263",List(KVCell("p_LEU","100000827984"))),
    HFileRow("CH~04223164",List(KVCell("p_LEU","100000459235"))),

    HFileRow("CH~01012444",List(KVCell("p_LEU","100000601835"))),

    HFileRow("LEU~100000246017",List(KVCell("c_00032262","CH"), KVCell("c_111222333","VAT"), KVCell("c_1152L","PAYE"), KVCell("c_1153L","PAYE"), KVCell("p_ENT","3000000011"))),
    HFileRow("LEU~100000459235",List(KVCell("c_04223164","CH"), KVCell("c_1166L","PAYE"), KVCell("c_222666000","VAT"), KVCell("c_555666777","VAT"), KVCell("p_ENT","4000000011"))),
    HFileRow("LEU~100000508723",List(KVCell("c_04223165","CH"), KVCell("c_111000111","VAT"), KVCell("c_1188L","PAYE"), KVCell("c_1199L","PAYE"), KVCell("p_ENT","4000000011"))),
    HFileRow("LEU~100000508724",List(KVCell("c_00012345","CH"), KVCell("c_5555L","PAYE"), KVCell("c_999888777","VAT"), KVCell("p_ENT","4000000011"))),
    HFileRow("LEU~100000827984",List(KVCell("c_00032263","CH"), KVCell("c_1154L","PAYE"), KVCell("c_1155L","PAYE"), KVCell("c_222333444","VAT"), KVCell("p_ENT","3000000011"))),

    HFileRow("LEU~100000601835",List(KVCell("c_01012444","CH"), KVCell("c_9876L","PAYE"), KVCell("c_555666777003","VAT"), KVCell("p_ENT","5000000011"))),

    HFileRow("LEU~100002826247",List(KVCell("c_00032261","CH"), KVCell("c_1151L","PAYE"), KVCell("c_123123123","VAT"), KVCell("p_ENT",entWithMissingLouId))),
    HFileRow("VAT~111000111",List(KVCell("p_LEU","100000508723"))),
    HFileRow("VAT~111222333",List(KVCell("p_LEU","100000246017"))),

    HFileRow("VAT~555666777003",List(KVCell("p_LEU","100000601835"))),

    HFileRow("PAYE~1151L",List(KVCell("p_LEU","100002826247"))),
    HFileRow("PAYE~1152L",List(KVCell("p_LEU","100000246017"))),
    HFileRow("PAYE~1153L",List(KVCell("p_LEU","100000246017"))),
    HFileRow("PAYE~1154L",List(KVCell("p_LEU","100000827984"))),
    HFileRow("PAYE~1155L",List(KVCell("p_LEU","100000827984"))),
    HFileRow("PAYE~1166L",List(KVCell("p_LEU","100000459235"))),
    HFileRow("PAYE~1177L",List(KVCell("p_LEU","100000459235"))),
    HFileRow("PAYE~1188L",List(KVCell("p_LEU","100000508723"))),
    HFileRow("PAYE~1199L",List(KVCell("p_LEU","100000508723"))),

    HFileRow("PAYE~9876L",List(KVCell("p_LEU","100000601835"))),

    HFileRow("VAT~123123123",List(KVCell("p_LEU","100002826247"))),
    HFileRow(s"ENT~5000000011",List(KVCell("c_100000601835","LEU"), KVCell("c_550000088","LOU"), KVCell("c_6000000006","REU"))),
    HFileRow(s"ENT~$entWithMissingLouId",List(KVCell("c_100002826247","LEU"), KVCell(s"c_$missingLouLurn","LOU"))),
    HFileRow(s"LOU~$missingLouLurn",List(KVCell("p_ENT",entWithMissingLouId))),
    HFileRow("VAT~222333444",List(KVCell("p_LEU","100000827984"))),
    HFileRow("VAT~222666000",List(KVCell("p_LEU","100000459235"))),
    HFileRow("ENT~3000000011",List(KVCell("c_100000246017","LEU"), KVCell("c_100000827984","LEU"), KVCell("c_300000088","LOU"), KVCell("c_300000099","LOU"))),
    HFileRow("LOU~300000055",List(KVCell("p_ENT","4000000011"))),
    HFileRow("LOU~300000066",List(KVCell("p_ENT","4000000011"))),
    HFileRow("LOU~300000077",List(KVCell("p_ENT","4000000011"))),
    HFileRow("LOU~300000088",List(KVCell("p_ENT","3000000011"))),
    HFileRow("LOU~300000099",List(KVCell("p_ENT","3000000011"))),

    HFileRow("LOU~550000088",List(KVCell("p_ENT","5000000011"))),

    HFileRow("ENT~4000000011",List(KVCell("c_100000459235","LEU"), KVCell("c_100000508723","LEU"), KVCell("c_100000508724","LEU"), KVCell("c_300000055","LOU"), KVCell("c_300000066","LOU"), KVCell("c_300000077","LOU"))),
    HFileRow("PAYE~5555L",List(KVCell("p_LEU","100000508724"))),
    HFileRow("VAT~555666777",List(KVCell("p_LEU","100000459235"))),
    HFileRow("VAT~999888777",List(KVCell("p_LEU","100000508724"))),
    HFileRow("CH~00032261",List(KVCell("p_LEU","100002826247")))  //this will be deleted with update
  )

  val existingLinksForMissingLousScenario = List(
    HFileRow("00012345~CH",List(KVCell("p_LEU","100000508724"))),
    HFileRow("00032262~CH",List(KVCell("p_LEU","100000246017"))),
    HFileRow("04223165~CH",List(KVCell("p_LEU","100000508723"))),
    HFileRow("00032263~CH",List(KVCell("p_LEU","100000827984"))),
    HFileRow("04223164~CH",List(KVCell("p_LEU","100000459235"))),
    HFileRow("100000246017~LEU",List(KVCell("c_00032262","CH"), KVCell("c_111222333","VAT"), KVCell("c_1152L","PAYE"), KVCell("c_1153L","PAYE"), KVCell("p_ENT","3000000011"))),
    HFileRow("100000459235~LEU",List(KVCell("c_04223164","CH"), KVCell("c_1166L","PAYE"), KVCell("c_222666000","VAT"), KVCell("c_555666777","VAT"), KVCell("p_ENT","4000000011"))),
    HFileRow("100000508723~LEU",List(KVCell("c_04223165","CH"), KVCell("c_111000111","VAT"), KVCell("c_1188L","PAYE"), KVCell("c_1199L","PAYE"), KVCell("p_ENT","4000000011"))),
    HFileRow("100000508724~LEU",List(KVCell("c_00012345","CH"), KVCell("c_5555L","PAYE"), KVCell("c_999888777","VAT"), KVCell("p_ENT","4000000011"))),
    HFileRow("100000827984~LEU",List(KVCell("c_00032263","CH"), KVCell("c_1154L","PAYE"), KVCell("c_1155L","PAYE"), KVCell("c_222333444","VAT"), KVCell("p_ENT","3000000011"))),
    HFileRow("100002826247~LEU",List(KVCell("c_00032261","CH"), KVCell("c_1151L","PAYE"), KVCell("c_123123123","VAT"), KVCell("p_ENT",entWithMissingLouId))),
    HFileRow("111000111~VAT",List(KVCell("p_LEU","100000508723"))),
    HFileRow("111222333~VAT",List(KVCell("p_LEU","100000246017"))),
    HFileRow("PAYE~1151L",List(KVCell("p_LEU","100002826247"))),
    HFileRow("PAYE~1152L",List(KVCell("p_LEU","100000246017"))),
    HFileRow("PAYE~1153L",List(KVCell("p_LEU","100000246017"))),
    HFileRow("PAYE~1154L",List(KVCell("p_LEU","100000827984"))),
    HFileRow("PAYE~1155L",List(KVCell("p_LEU","100000827984"))),
    HFileRow("PAYE~1166L",List(KVCell("p_LEU","100000459235"))),
    HFileRow("PAYE~1177L",List(KVCell("p_LEU","100000459235"))),
    HFileRow("PAYE~1188L",List(KVCell("p_LEU","100000508723"))),
    HFileRow("PAYE~1199L",List(KVCell("p_LEU","100000508723"))),
    HFileRow("VAT~123123123",List(KVCell("p_LEU","100002826247"))),
    HFileRow(s"ENT~$entWithMissingLouId",List(KVCell("c_100002826247","LEU"))),
    HFileRow("VAT~222333444",List(KVCell("p_LEU","100000827984"))),
    HFileRow("VAT~222666000",List(KVCell("p_LEU","100000459235"))),
    HFileRow("ENT~3000000011",List(KVCell("c_100000246017","LEU"), KVCell("c_100000827984","LEU"), KVCell("c_300000088","LOU"), KVCell("c_300000099","LOU"))),
    HFileRow("LOU~300000055",List(KVCell("p_ENT","4000000011"))),
    HFileRow("LOU~300000066",List(KVCell("p_ENT","4000000011"))),
    HFileRow("LOU~300000077",List(KVCell("p_ENT","4000000011"))),
    HFileRow("LOU~300000088",List(KVCell("p_ENT","3000000011"))),
    HFileRow("LOU~300000099",List(KVCell("p_ENT","3000000011"))),
    HFileRow("ENT~4000000011",List(KVCell("c_100000459235","LEU"), KVCell("c_100000508723","LEU"), KVCell("c_100000508724","LEU"), KVCell("c_300000055","LOU"), KVCell("c_300000066","LOU"), KVCell("c_300000077","LOU"))),
    HFileRow("PAYE~5555L",List(KVCell("p_LEU","100000508724"))),
    HFileRow("VAT~555666777",List(KVCell("p_LEU","100000459235"))),
    HFileRow("VAT~999888777",List(KVCell("p_LEU","100000508724"))),
    HFileRow("VH~00032261",List(KVCell("p_LEU","100002826247")))  //this will be deleted with update
  )

}
