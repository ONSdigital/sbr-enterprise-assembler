package dao.parquet

import model.domain.{HFileRow, KVCell}

trait NewPeriodWithNewMissingLouLinks {



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
  HFileRow("100002826247~LEU~201804",List(KVCell("c_1151L","PAYE"),  KVCell("c_123123123","VAT"), KVCell("p_ENT","2000000011")).sortBy(_.column)),
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
  HFileRow("2000000011~ENT~201804",List(KVCell("c_100002826247","LEU"), KVCell("c_888888888","LOU")).sortBy(_.column)),
  HFileRow("888888888~LOU~201804",List(KVCell("p_ENT","2000000011"))),
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
  HFileRow("5000000011~ENT~201804",List(KVCell("c_500000099","LOU"), KVCell("c_999000508999","LEU"))), //new enterprise
  HFileRow("500000099~LOU~201804",List(KVCell("p_ENT","5000000011"))),
  HFileRow("5555L~PAYE~201804",List(KVCell("p_LEU","100000508724"))),
  HFileRow("555666777~VAT~201804",List(KVCell("p_LEU","100000459235"))),
  HFileRow("919100010~VAT~201804",List(KVCell("p_LEU","999000508999"))), //new, added with new LEU(and hence ENT and LOU)
                                       /// CH c_33322444 added with update, see line 48
  HFileRow("999000508999~LEU~201804",List(KVCell("c_33322444","CH"), KVCell("c_919100010","VAT"), KVCell("p_ENT","5000000011")).sortBy(_.column)), // new LEU
  HFileRow("999888777~VAT~201804",List(KVCell("p_LEU","100000508724")))
)}
