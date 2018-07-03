package dao.parquet
import model.domain.{HFileRow, KVCell}


trait ExistingLinks {

//     HFileRow("00032261~CH~201803",List(KVCell("p_LEU","100002826247"))),

  val existingLinksForAddNewPeriodScenarion = List(
    HFileRow("00012345~CH~201803",List(KVCell("p_LEU","100000508724"))),
    HFileRow("00032262~CH~201803",List(KVCell("p_LEU","100000246017"))),
    HFileRow("04223165~CH~201803",List(KVCell("p_LEU","100000508723"))),
    HFileRow("00032263~CH~201803",List(KVCell("p_LEU","100000827984"))),
    HFileRow("04223164~CH~201803",List(KVCell("p_LEU","100000459235"))),
    HFileRow("100000246017~LEU~201803",List(KVCell("c_00032262","CH"), KVCell("c_111222333","VAT"), KVCell("c_1152L","PAYE"), KVCell("c_1153L","PAYE"), KVCell("p_ENT","3000000011"))),
    HFileRow("100000459235~LEU~201803",List(KVCell("c_04223164","CH"), KVCell("c_1166L","PAYE"), KVCell("c_222666000","VAT"), KVCell("c_555666777","VAT"), KVCell("p_ENT","4000000011"))),
    HFileRow("100000508723~LEU~201803",List(KVCell("c_04223165","CH"), KVCell("c_111000111","VAT"), KVCell("c_1188L","PAYE"), KVCell("c_1199L","PAYE"), KVCell("p_ENT","4000000011"))),
    HFileRow("100000508724~LEU~201803",List(KVCell("c_00012345","CH"), KVCell("c_5555L","PAYE"), KVCell("c_999888777","VAT"), KVCell("p_ENT","4000000011"))),
    HFileRow("100000827984~LEU~201803",List(KVCell("c_00032263","CH"), KVCell("c_1154L","PAYE"), KVCell("c_1155L","PAYE"), KVCell("c_222333444","VAT"), KVCell("p_ENT","3000000011"))),
    HFileRow("100002826247~LEU~201803",List(KVCell("c_00032261","CH"), KVCell("c_1151L","PAYE"), KVCell("c_123123123","VAT"), KVCell("p_ENT","2000000011"))),
    HFileRow("111000111~VAT~201803",List(KVCell("p_LEU","100000508723"))),
    HFileRow("111222333~VAT~201803",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1151L~PAYE~201803",List(KVCell("p_LEU","100002826247"))),
    HFileRow("1152L~PAYE~201803",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1153L~PAYE~201803",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1154L~PAYE~201803",List(KVCell("p_LEU","100000827984"))),
    HFileRow("1155L~PAYE~201803",List(KVCell("p_LEU","100000827984"))),
    HFileRow("1166L~PAYE~201803",List(KVCell("p_LEU","100000459235"))),
    HFileRow("1177L~PAYE~201803",List(KVCell("p_LEU","100000459235"))),
    HFileRow("1188L~PAYE~201803",List(KVCell("p_LEU","100000508723"))),
    HFileRow("1199L~PAYE~201803",List(KVCell("p_LEU","100000508723"))),
    HFileRow("123123123~VAT~201803",List(KVCell("p_LEU","100002826247"))),
    HFileRow("2000000011~ENT~201803",List(KVCell("c_100002826247","LEU"), KVCell("c_200000099","LOU"))),
    HFileRow("200000099~LOU~201803",List(KVCell("p_ENT","2000000011"))),
    HFileRow("222333444~VAT~201803",List(KVCell("p_LEU","100000827984"))),
    HFileRow("222666000~VAT~201803",List(KVCell("p_LEU","100000459235"))),
    HFileRow("3000000011~ENT~201803",List(KVCell("c_100000246017","LEU"), KVCell("c_100000827984","LEU"), KVCell("c_300000088","LOU"), KVCell("c_300000099","LOU"))),
    HFileRow("300000055~LOU~201803",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000066~LOU~201803",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000077~LOU~201803",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000088~LOU~201803",List(KVCell("p_ENT","3000000011"))),
    HFileRow("300000099~LOU~201803",List(KVCell("p_ENT","3000000011"))),
    HFileRow("4000000011~ENT~201803",List(KVCell("c_100000459235","LEU"), KVCell("c_100000508723","LEU"), KVCell("c_100000508724","LEU"), KVCell("c_300000055","LOU"), KVCell("c_300000066","LOU"), KVCell("c_300000077","LOU"))),
    HFileRow("5555L~PAYE~201803",List(KVCell("p_LEU","100000508724"))),
    HFileRow("555666777~VAT~201803",List(KVCell("p_LEU","100000459235"))),
    HFileRow("999888777~VAT~201803",List(KVCell("p_LEU","100000508724"))),
    HFileRow("00032261~CH~201803",List(KVCell("p_LEU","100002826247")))  //this will be deleted with update
  )

  val existingLinksForMissingLousScenario = List(
    HFileRow("00012345~CH~201803",List(KVCell("p_LEU","100000508724"))),
    HFileRow("00032262~CH~201803",List(KVCell("p_LEU","100000246017"))),
    HFileRow("04223165~CH~201803",List(KVCell("p_LEU","100000508723"))),
    HFileRow("00032263~CH~201803",List(KVCell("p_LEU","100000827984"))),
    HFileRow("04223164~CH~201803",List(KVCell("p_LEU","100000459235"))),
    HFileRow("100000246017~LEU~201803",List(KVCell("c_00032262","CH"), KVCell("c_111222333","VAT"), KVCell("c_1152L","PAYE"), KVCell("c_1153L","PAYE"), KVCell("p_ENT","3000000011"))),
    HFileRow("100000459235~LEU~201803",List(KVCell("c_04223164","CH"), KVCell("c_1166L","PAYE"), KVCell("c_222666000","VAT"), KVCell("c_555666777","VAT"), KVCell("p_ENT","4000000011"))),
    HFileRow("100000508723~LEU~201803",List(KVCell("c_04223165","CH"), KVCell("c_111000111","VAT"), KVCell("c_1188L","PAYE"), KVCell("c_1199L","PAYE"), KVCell("p_ENT","4000000011"))),
    HFileRow("100000508724~LEU~201803",List(KVCell("c_00012345","CH"), KVCell("c_5555L","PAYE"), KVCell("c_999888777","VAT"), KVCell("p_ENT","4000000011"))),
    HFileRow("100000827984~LEU~201803",List(KVCell("c_00032263","CH"), KVCell("c_1154L","PAYE"), KVCell("c_1155L","PAYE"), KVCell("c_222333444","VAT"), KVCell("p_ENT","3000000011"))),
    HFileRow("100002826247~LEU~201803",List(KVCell("c_00032261","CH"), KVCell("c_1151L","PAYE"), KVCell("c_123123123","VAT"), KVCell("p_ENT","2000000011"))),
    HFileRow("111000111~VAT~201803",List(KVCell("p_LEU","100000508723"))),
    HFileRow("111222333~VAT~201803",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1151L~PAYE~201803",List(KVCell("p_LEU","100002826247"))),
    HFileRow("1152L~PAYE~201803",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1153L~PAYE~201803",List(KVCell("p_LEU","100000246017"))),
    HFileRow("1154L~PAYE~201803",List(KVCell("p_LEU","100000827984"))),
    HFileRow("1155L~PAYE~201803",List(KVCell("p_LEU","100000827984"))),
    HFileRow("1166L~PAYE~201803",List(KVCell("p_LEU","100000459235"))),
    HFileRow("1177L~PAYE~201803",List(KVCell("p_LEU","100000459235"))),
    HFileRow("1188L~PAYE~201803",List(KVCell("p_LEU","100000508723"))),
    HFileRow("1199L~PAYE~201803",List(KVCell("p_LEU","100000508723"))),
    HFileRow("123123123~VAT~201803",List(KVCell("p_LEU","100002826247"))),
    HFileRow("2000000011~ENT~201803",List(KVCell("c_100002826247","LEU"))),
    HFileRow("222333444~VAT~201803",List(KVCell("p_LEU","100000827984"))),
    HFileRow("222666000~VAT~201803",List(KVCell("p_LEU","100000459235"))),
    HFileRow("3000000011~ENT~201803",List(KVCell("c_100000246017","LEU"), KVCell("c_100000827984","LEU"), KVCell("c_300000088","LOU"), KVCell("c_300000099","LOU"))),
    HFileRow("300000055~LOU~201803",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000066~LOU~201803",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000077~LOU~201803",List(KVCell("p_ENT","4000000011"))),
    HFileRow("300000088~LOU~201803",List(KVCell("p_ENT","3000000011"))),
    HFileRow("300000099~LOU~201803",List(KVCell("p_ENT","3000000011"))),
    HFileRow("4000000011~ENT~201803",List(KVCell("c_100000459235","LEU"), KVCell("c_100000508723","LEU"), KVCell("c_100000508724","LEU"), KVCell("c_300000055","LOU"), KVCell("c_300000066","LOU"), KVCell("c_300000077","LOU"))),
    HFileRow("5555L~PAYE~201803",List(KVCell("p_LEU","100000508724"))),
    HFileRow("555666777~VAT~201803",List(KVCell("p_LEU","100000459235"))),
    HFileRow("999888777~VAT~201803",List(KVCell("p_LEU","100000508724"))),
    HFileRow("00032261~CH~201803",List(KVCell("p_LEU","100002826247")))  //this will be deleted with update
  )


  val existingLinksForCreateNewPopulationScenarion = List(

    HFileRow("testLocalUnitId-11111~LOU~201802",List(KVCell("p_ENT","testEnterpriseId-22222"))),
    HFileRow("testLocalUnitId-22222~LOU~201802",List(KVCell("p_ENT","testEnterpriseId-11111"))),
    HFileRow("testLocalUnitId-33333~LOU~201802",List(KVCell("p_ENT","testEnterpriseId-33333"))),
    HFileRow("testEnterpriseId-22222~ENT~201802",List(KVCell("c_15931638","LEU"), KVCell("testLocalUnitId-11111","LOU"))),
    HFileRow("21840175~LEU~201802",List(KVCell("c_10000","VAT"),KVCell("c_20000","VAT"), KVCell("p_ENT","testEnterpriseId-11111"))),
    HFileRow("testEnterpriseId-11111~ENT~201802",List(KVCell("c_21840175","LEU"), KVCell("testLocalUnitId-22222","LOU"))),
    HFileRow("testEnterpriseId-33333~ENT~201802",List(KVCell("c_28919372","LEU"), KVCell("testLocalUnitId-33333","LOU"))),
    HFileRow("15931638~LEU~201802",List(KVCell("c_10002","VAT"), KVCell("c_12345W","PAYE"), KVCell("c_ERT12","PAYE"), KVCell("c_SZ124306","CH"), KVCell("p_ENT","testEnterpriseId-22222"))),
    HFileRow("28919372~LEU~201802",List(KVCell("c_20002","PAYE"), KVCell("c_30003","PAYE") , KVCell("p_ENT","testEnterpriseId-33333"))),
    HFileRow("12345W~PAYE~201802",List(KVCell("p_LEU","15931638"))),
    HFileRow("10002~VAT~201802",List(KVCell("p_LEU","15931638"))),
    HFileRow("10000~VAT~201802",List(KVCell("p_LEU","21840175"))),
    HFileRow("20000~VAT~201802",List(KVCell("p_LEU","21840175"))),
    HFileRow("30003~PAYE~201802",List(KVCell("p_LEU","28919372"))),
    HFileRow("SZ124306~CH~201802",List(KVCell("p_LEU","15931638"))),
    HFileRow("20002~PAYE~201802",List(KVCell("p_LEU","28919372"))),
    HFileRow("ERT12~PAYE~201802",List(KVCell("p_LEU","15931638")))

  )

}