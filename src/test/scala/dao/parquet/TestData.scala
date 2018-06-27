package dao.parquet

import model.domain.{Enterprise, HFileRow, KVCell, LocalUnit}

/**
  *
  */
trait TestData { 

/**
  * creates Array[Ent] for matching actual results returned by HBase.
  * As ENT keys generated dynamically and cannot be matched, the keys are copied from actual results so that the rest of Ent object's attribute values
  * can be checked for equality
  * */

  val newEntErn = "5000000011"
  val newLouLurn = "500000099"

  /*def testEnterprisesSmallWithNullValues(ents:Seq[Enterprise]) = {

    def getKeyByName(name:String): String =
      ents.collect{case Enterprise(ern,_,Some(`name`),_,_,_,_,_,_,_,_,_,_) => ern}.head

      Seq(
        Enterprise(getKeyByName("MERCATURA INVESTMENTS LIMITED") ,Some("9999999999"),Some("MERCATURA INVESTMENTS LIMITED"),Some("FS20 3OS"),Some("6"),Some("70176"),Some("8"),Some("10"),None,Some("45"),Some("45"),None,None),
        Enterprise(getKeyByName("JETMORE DEVELOPMENTS LIMITED") ,Some("9999999999"),Some("JETMORE DEVELOPMENTS LIMITED"),Some("OK16 5XQ"),Some("4"),Some("90125"),Some("4"), Some(""),None,None,None,None,None),
        Enterprise(getKeyByName("5TH PROPERTY TRADING LIMITED") ,Some("9999999999"),Some("5TH PROPERTY TRADING LIMITED"),Some("HQ92 3GV"),Some("3"),Some("90481"),Some("2"),Some("4"),None,None,None,None,None),
        Enterprise(getKeyByName("NBD CONTRACTS LIMITED") ,Some("9999999999"),Some("NBD CONTRACTS LIMITED"),Some("UT10 7BS"),Some("5"),Some("10111"),Some("5"),Some("2"),None,None,None,None,None),
        Enterprise(getKeyByName("PURPLE SKY FUTURES LTD") ,Some("9999999999"),Some("PURPLE SKY FUTURES LTD"),Some("HG33 4OY"),Some("7"),Some("20222"),Some(""),Some(""),None,None,None,None,None),
        Enterprise(getKeyByName("ACCLAIMED HOMES LIMITED") ,Some("9999999999"),Some("ACCLAIMED HOMES LIMITED"),Some("LB07 6UT"),Some("3"),Some("00742"),None,None,None,Some("85"),Some("85"),None,None),
        Enterprise(getKeyByName("RALPH GROUP LIMITED") ,Some("9999999999"),Some("RALPH GROUP LIMITED"),Some("SI83 9RT"),Some("6"),Some("30333"),Some(""),Some(""),None,None,None,None,None),
        Enterprise(getKeyByName("BCM TRADING LIMITED") ,Some("9999999999"),Some("BCM TRADING LIMITED"),Some("RM91 8SJ"),Some("1"),Some("40444"),Some("6"),Some("8"),None,None,None,None,None),
        Enterprise(getKeyByName("GREAT GLEN CONSULTING LTD") ,Some("9999999999"),Some("GREAT GLEN CONSULTING LTD"),Some("MA61 3KB"),Some("7"),Some("50555"),Some(""),Some(""),None,None,None,None,None),
        Enterprise(getKeyByName("TORUS DEVELOPMENT CONSULTANTS LIMITED") ,Some("9999999999"),Some("TORUS DEVELOPMENT CONSULTANTS LIMITED"),Some("FM25 8QP"),Some("7"),Some("60666"),Some(""),None,None,None,None,None,None)
    )
  }*/

  def testEnterprises3Recs(ents:Seq[Enterprise] ) = {

    def getKeyByName(name:String): String =
      ents.collect{case Enterprise(ern,_,`name`,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_) => ern}.head

    Seq(
/*   Enterprise("2000000011",Some("9900000009"),"INDUSTRIES LTD","WHITE LANE",None,None,None,None,"B22 2TL",None,"12345","2",Some("4"),None,None,None,None,None,None),
    Enterprise("3000000011",Some("9900000126"),"BLACKWELLGROUP LTD","GOGGESHALL ROAD",None,None,None,None, "CO6 2JX",None,"23456","17",Some("20"),None,None,Some("1175"),Some("585"),Some("590"),None),
    Enterprise("4000000011",Some("9900000242"),"IBM LTD","BSTER DEPT",None,None,None,None,"PO6 3AU",None,"34567","4",Some("8"),None,None,Some("180"),Some("180"),None,None),*/
      Enterprise(getKeyByName("MERCATURA INVESTMENTS LIMITED") ,Some("9999999999"),"MERCATURA INVESTMENTS LIMITED","",None,None,None,None,"FS20 3OS",Some("6"),"70176","8",Some("10"),None,Some("45"),Some("45"),None,None,None),
      Enterprise(getKeyByName("ACCLAIMED HOMES LIMITED") ,Some("9999999999"),"ACCLAIMED HOMES LIMITED","",None,None,None,None,"LB07 6UT",Some("3"),"00742","6",None,None,Some("85"),Some("85"),None,None,None),
      Enterprise(getKeyByName("5TH PROPERTY TRADING LIMITED") ,Some("9999999999"),"5TH PROPERTY TRADING LIMITED","",None,None,None,None,"HQ92 3GV",Some("3"),"90481","2", Some("4"),None,None,None,None,None,None)

    )
  }

  def testLocalUnits3Recs(ents:Seq[LocalUnit] ) = {
/*lurn:String,	luref:Option[String],	ern:String, entref:Option[String],	name:String,	tradingstyle:Option[String],
                     address1:String,	address2:Option[String],	address3:Option[String],address4:Option[String],address5:Option[String],
                     postcode:String,sic07:String,employees:String*/

    def getIdsByName(name:String): (String,String) =
      ents.collect{case LocalUnit(lurn, _, ern,_,name_,_,_,_,_,_,_,_,_,_) => (lurn,ern)}.head

    Seq[LocalUnit](
      new LocalUnit(
        lurn="testLocalUnitId-11111",
        luref=None,
        ern="testEnterpriseId-11111",
        entref = None,
        name = "5TH PROPERTY TRADING LIMITED",
        tradingstyle = None,
        address1 = "",
        address2 = None,
        address3 = None,
        address4 = None,
        address5 = None,
        postcode = "HQ92 3GV",
        sic07 = "90481",
        employees = "0"
      ),
      new LocalUnit(
        lurn="testLocalUnitId-22222",
        luref=None,
        ern="testEnterpriseId-22222",
        entref = None,
        name = "ACCLAIMED HOMES LIMITED",
        tradingstyle = None,
        address1 = "",
        address2 = None,
        address3 = None,
        address4 = None,
        address5 = None,
        postcode = "LB07 6UT",
        sic07 = "00742",
        employees = "0"
      ),
      new LocalUnit(
        /*HFileRow(33333-dIesirpretnEtset~201802~testLocalUnitId-33333,List(KVCell(address1,), KVCell(employees,0), KVCell(ern,testEnterpriseId-33333), KVCell(lurn,testLocalUnitId-33333), KVCell(name,MERCATURA INVESTMENTS LIMITED), KVCell(postcode,FS20 3OS), KVCell(sic07,70176)))*/
        lurn="testLocalUnitId-33333",
        luref=None,
        ern="testEnterpriseId-33333",
        entref = None,
        name = "MERCATURA INVESTMENTS LIMITED",
        tradingstyle = None,
        address1 = "",
        address2 = None,
        address3 = None,
        address4 = None,
        address5 = None,
        postcode = "FS20 3OS",
        sic07 = "70176",
        employees = "0"
      )

      /*HFileRow(11111-dIesirpretnEtset~201802~testLocalUnitId-11111,List(KVCell(address1,), KVCell(employees,0), KVCell(ern,testEnterpriseId-11111), KVCell(lurn,testLocalUnitId-11111), KVCell(name,5TH PROPERTY TRADING LIMITED), KVCell(postcode,HQ92 3GV), KVCell(sic07,90481)))*/

    )
  }

  def testLinkRowsSmallWithNullValues(entLinks:Seq[HFileRow]) = List(
                HFileRow("27280354~CH~201802",List(KVCell("p_LEU","10544190"))),
                HFileRow("12345W~PAYE~201802",List(KVCell("p_LEU","15931638"))),
                HFileRow("15931638~LEU~201802",List(KVCell("p_ENT","4KiXL1hlDH3GGnbbKq"), KVCell("c_12345W","PAYE"), KVCell("c_10002","VAT"), KVCell("c_SZ124306","CH"), KVCell("c_ERT12","PAYE"))),
                HFileRow("56327266~LEU~201802",List(KVCell("c_10008","VAT"), KVCell("p_ENT","OXzCYJKOjGxLjMlORp"), KVCell("c_DV823168","CH"), KVCell("c_20016","PAYE"))),
                HFileRow("5w8571akKLNLG9w5nK~ENT~201802",List(KVCell("c_60899120","LEU"))),
                HFileRow("21840175~LEU~201802",List(KVCell("c_10000","VAT"), KVCell("p_ENT","IkFphXcsrZhdiPnHK5"), KVCell("c_20000","VAT"))),
                HFileRow("JK682461~CH~201802",List(KVCell("p_LEU","60899120"))),
                HFileRow("28919372~LEU~201802",List(KVCell("c_20002","PAYE"), KVCell("c_30003","PAYE"), KVCell("p_ENT","NcIYq7IymWXbAXckWL"))),
                HFileRow("74515246~CH~201802",List(KVCell("p_LEU","99433188"))),
                HFileRow("ERT12~PAYE~201802",List(KVCell("p_LEU","15931638"))),
                HFileRow("IkFphXcsrZhdiPnHK5~ENT~201802",List(KVCell("c_21840175","LEU"))),
                HFileRow("10000~VAT~201802",List(KVCell("p_LEU","21840175"))),
                HFileRow("xhDzhsZ4HW9piViu7K~ENT~201802",List(KVCell("c_99433188","LEU"))),
                HFileRow("69016123~LEU~201802",List(KVCell("p_ENT","DgfU5FlZVqlfnmO5vL"), KVCell("c_20010","PAYE"), KVCell("c_53806114","CH"), KVCell("c_10005","VAT"))),
                HFileRow("30003~PAYE~201802",List(KVCell("p_LEU","28919372"))),
                HFileRow("10002~VAT~201802",List(KVCell("p_LEU","15931638"))),
                HFileRow("TT301000~CH~201802",List(KVCell("p_LEU","86883196"))),
                HFileRow("10003~VAT~201802",List(KVCell("p_LEU","38557538"))),
                HFileRow("FITAfXVNPZO8WPpbBp~ENT~201802",List(KVCell("c_10544190","LEU"))),
                HFileRow("10004~VAT~201802",List(KVCell("p_LEU","60899120"))),
                HFileRow("DV823168~CH~201802",List(KVCell("p_LEU","56327266"))),
                HFileRow("10005~VAT~201802",List(KVCell("p_LEU","69016123"))),
                HFileRow("bUKhGsRXfNsr5noO1n~ENT~201802",List(KVCell("c_86883196","LEU"))),
                HFileRow("SZ124306~CH~201802",List(KVCell("p_LEU","15931638"))),
                HFileRow("20018~PAYE~201802",List(KVCell("p_LEU","10544190"))),
                HFileRow("20016~PAYE~201802",List(KVCell("p_LEU","56327266"))),
                HFileRow("10006~VAT~201802",List(KVCell("p_LEU","86883196"))),
                HFileRow("20010~PAYE~201802",List(KVCell("p_LEU","69016123"))),
                HFileRow("20012~PAYE~201802",List(KVCell("p_LEU","86883196"))),
                HFileRow("OXzCYJKOjGxLjMlORp~ENT~201802",List(KVCell("c_56327266","LEU"))),
                HFileRow("38557538~LEU~201802",List(KVCell("p_ENT","mw5OqAt4ftPlwVyTSm"), KVCell("c_20006","PAYE"), KVCell("c_10003","VAT"), KVCell("c_09432504","CH"))),
                HFileRow("09432504~CH~201802",List(KVCell("p_LEU","38557538"))),
                HFileRow("4KiXL1hlDH3GGnbbKq~ENT~201802",List(KVCell("c_15931638","LEU"))),
                HFileRow("DgfU5FlZVqlfnmO5vL~ENT~201802",List(KVCell("c_69016123","LEU"))),
                HFileRow("20000~VAT~201802",List(KVCell("p_LEU","21840175"))),
                HFileRow("10008~VAT~201802",List(KVCell("p_LEU","56327266"))),
                HFileRow("NcIYq7IymWXbAXckWL~ENT~201802",List(KVCell("c_28919372","LEU"))),
                HFileRow("86883196~LEU~201802",List(KVCell("c_10006","VAT"), KVCell("c_TT301000","CH"), KVCell("c_20012","PAYE"), KVCell("p_ENT","bUKhGsRXfNsr5noO1n"))),
                HFileRow("53806114~CH~201802",List(KVCell("p_LEU","69016123"))),
                HFileRow("99433188~LEU~201802",List(KVCell("c_74515246","CH"), KVCell("p_ENT","xhDzhsZ4HW9piViu7K"))),
                HFileRow("60899120~LEU~201802",List(KVCell("c_JK682461","CH"), KVCell("c_20008","PAYE"), KVCell("c_10004","VAT"), KVCell("p_ENT","5w8571akKLNLG9w5nK"))),
                HFileRow("10009~VAT~201802",List(KVCell("p_LEU","10544190"))),
                HFileRow("10544190~LEU~201802",List(KVCell("c_10009","VAT"), KVCell("p_ENT","FITAfXVNPZO8WPpbBp"), KVCell("c_27280354","CH"), KVCell("c_20018","PAYE"))),
                HFileRow("mw5OqAt4ftPlwVyTSm~ENT~201802",List(KVCell("c_38557538","LEU"))),
                HFileRow("20008~PAYE~201802",List(KVCell("p_LEU","60899120"))),
                HFileRow("20002~PAYE~201802",List(KVCell("p_LEU","28919372"))),
                HFileRow("20006~PAYE~201802",List(KVCell("p_LEU","38557538")))
             ).sortBy(_.key)


  val testLocalUnitsRows3Recs = List(
    new LocalUnit(
      lurn="testLocalUnitId-11111",
      luref=None,
      ern="testEnterpriseId-11111",
      entref = None,
      name = "5TH PROPERTY TRADING LIMITED",
      tradingstyle = None,
      address1 = "",
      address2 = None,
      address3 = None,
      address4 = None,
      address5 = None,
      postcode = "HQ92 3GV",
      sic07 = "90481",
      employees = "0"
    ),
    new LocalUnit(
      lurn="testLocalUnitId-22222",
      luref=None,
      ern="testEnterpriseId-22222",
      entref = None,
      name = "ACCLAIMED HOMES LIMITED",
      tradingstyle = None,
      address1 = "",
      address2 = None,
      address3 = None,
      address4 = None,
      address5 = None,
      postcode = "LB07 6UT",
      sic07 = "00742",
      employees = "0"
    ),

    new LocalUnit(
      /*HFileRow(33333-dIesirpretnEtset~201802~testLocalUnitId-33333,List(KVCell(address1,), KVCell(employees,0), KVCell(ern,testEnterpriseId-33333), KVCell(lurn,testLocalUnitId-33333), KVCell(name,MERCATURA INVESTMENTS LIMITED), KVCell(postcode,FS20 3OS), KVCell(sic07,70176)))*/
      lurn="testLocalUnitId-33333",
      luref=None,
      ern="testEnterpriseId-33333",
      entref = None,
      name = "MERCATURA INVESTMENTS LIMITED",
      tradingstyle = None,
      address1 = "",
      address2 = None,
      address3 = None,
      address4 = None,
      address5 = None,
      postcode = "FS20 3OS",
      sic07 = "70176",
      employees = "0"
    )


  )

  val testLinkRows3Recs = List(

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


)//.sortBy((_.cells.map(_.column).mkString))

 /* ern:String, idbrref:Option[String],
  businessName:String,
  address1:String,
  address2:Option[String],
  address3:Option[String],
  address4:Option[String],
  address5:Option[String],
  PostCode:String,
  tradingStyle:Option[String],
  sic07:String,
  legalStatus:String,
  payeEmployees:Option[String],
  payeJobs:Option[String],
  appTurnover:Option[String],
  entTurnover:Option[String],
  cntdTurnover:Option[String],
  stdTurnover:Option[String],
  grp_turnover:Option[String]*/

  val newPeriodEnts = List(
    Enterprise("2000000011",Some("9900000009"),"INDUSTRIES LTD","WHITE LANE",None,None,None,None,"B22 2TL",None,"12345","2",Some("4"),None,None,None,None,None,None),
    Enterprise("3000000011",Some("9900000126"),"BLACKWELLGROUP LTD","GOGGESHALL ROAD",None,None,None,None, "CO6 2JX",None,"23456","17",Some("20"),None,None,Some("1175"),Some("585"),Some("590"),None),
    Enterprise("4000000011",Some("9900000242"),"IBM LTD","BSTER DEPT",None,None,None,None,"PO6 3AU",None,"34567","4",Some("8"),None,None,Some("180"),Some("180"),None,None),
    Enterprise(newEntErn,Some("9999999999"),"NEW ENTERPRISE LU","DEFAULT_VALUE", None,None,None,None, "W1A 1AA",Some("9"),"10001","1",None,None,None,Some("85"),Some("85"),None,None)
  )
  
  val newPeriodLocalUnits = List(
  LocalUnit("200000099",Some("100002826247"),"2000000011",Some("9900000009"),"INDUSTRIES LTD",None,"P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("WORCESTERSHIRE"),"B22 2TL","12345","2"),
  LocalUnit("300000088",Some("100000827984"),"3000000011",Some("9900000126"),"2-ND LU OF BLACKWELLGROUP LTD",None,"North End Rd lane",Some("Croydon"),Some("Surrey"),None,None,"CR0 1AA","1122","2"),
  LocalUnit("300000099",Some("100000246017"),"3000000011",Some("9900000126"),"BLACKWELLGROUP LTD",None,"GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX","23456","2"),
  LocalUnit("400000055",Some("100000508724"),"4000000011",Some("9900000242"),"3-RD LU OF IBM LTD",None,"IBM HOUSE",Some("Smile Street"),Some("Cardiff"),Some("SOUTH WALES"),None,"CF23 9EU","3344","1"),
  LocalUnit("400000066",Some("100000508723"),"4000000011",Some("9900000242"),"2-ND LU OF IBM LTD",None,"IT DEPT",Some("1 Hight Street"),Some("Newport"),Some("SOUTH WALES"),None,"NP10 6XG","2233","2"),
  LocalUnit("400000077",Some("100000459235"),"4000000011",Some("9900000242"),"IBM LTD",None,"BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"PO6 3AU","34567","2"),
  LocalUnit(newLouLurn, None, newEntErn, None, "NEW ENTERPRISE LU", None, "", None, None, None, None, "W1A 1AA", "10001", "0")
  )

  val newPeriodWithMissingLocalUnit = List(
  LocalUnit("200000099",None,"2000000011",Some("9900000009"),"INDUSTRIES LTD",None,"P O BOX 22",Some("INDUSTRIES HOUSE"),Some("WHITE LANE"),Some("REDDITCH"),Some("WORCESTERSHIRE"),"B22 2TL","12345","2"),
  LocalUnit("300000088",Some("100000827984"),"3000000011",Some("9900000126"),"2-ND LU OF BLACKWELLGROUP LTD",None,"North End Rd lane",Some("Croydon"),Some("Surrey"),None,None,"CR0 1AA","1122","2"),
  LocalUnit("300000099",Some("100000246017"),"3000000011",Some("9900000126"),"BLACKWELLGROUP LTD",None,"GOGGESHALL ROAD",Some("EARLS COLNE"),Some("COLCHESTER"),None,None,"CO6 2JX","23456","2"),
  LocalUnit("400000055",Some("100000508724"),"4000000011",Some("9900000242"),"3-RD LU OF IBM LTD",None,"IBM HOUSE",Some("Smile Street"),Some("Cardiff"),Some("SOUTH WALES"),None,"CF23 9EU","3344","1"),
  LocalUnit("400000066",Some("100000508723"),"4000000011",Some("9900000242"),"2-ND LU OF IBM LTD",None,"IT DEPT",Some("1 Hight Street"),Some("Newport"),Some("SOUTH WALES"),None,"NP10 6XG","2233","2"),
  LocalUnit("400000077",Some("100000459235"),"4000000011",Some("9900000242"),"IBM LTD",None,"BSTER DEPT",Some("MAILPOINT A1F"),Some("P O BOX 41"),Some("NORTH HARBOUR"),Some("PORTSMOUTH"),"PO6 3AU","34567","2"),
  LocalUnit(newLouLurn, None, newEntErn, None, "NEW ENTERPRISE LU", None, "", None, None, None, None, "W1A 1AA", "10001", "0")
  )

}
