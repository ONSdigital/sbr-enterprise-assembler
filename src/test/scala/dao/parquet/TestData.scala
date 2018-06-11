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
  def testEnterprisesSmallWithNullValues(ents:Seq[Enterprise]) = {

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
  }

  def testEnterprises3Recs(ents:Seq[Enterprise] ) = {

    def getKeyByName(name:String): String =
      ents.collect{case Enterprise(ern,_,Some(`name`),_,_,_,_,_,_,_,_,_,_) => ern}.head

    Seq(
      Enterprise(getKeyByName("MERCATURA INVESTMENTS LIMITED") ,Some("9999999999"),Some("MERCATURA INVESTMENTS LIMITED"),Some("FS20 3OS"),Some("6"),Some("70176"),Some("8"),Some("10"),None,Some("45"),Some("45"),None,None),
      Enterprise(getKeyByName("ACCLAIMED HOMES LIMITED") ,Some("9999999999"),Some("ACCLAIMED HOMES LIMITED"),Some("LB07 6UT"),Some("3"),Some("00742"),None,None,None,Some("85"),Some("85"),None,None),
      Enterprise(getKeyByName("5TH PROPERTY TRADING LIMITED") ,Some("9999999999"),Some("5TH PROPERTY TRADING LIMITED"),Some("HQ92 3GV"),Some("3"),Some("90481"),Some("2"),Some("4"),None,None,None,None,None)

    )
  }

  def testLocalUnits3Recs(lous:Seq[LocalUnit] ) = {
/*entref:Option[String],	name:String,	tradingstyle:Option[String],
                     address1:String,	address2:Option[String],	address3:Option[String],address4:Option[String],address5:Option[String],
                     postcode:String,sic07:String,employees:String*/
    def getKeyByName(name:String): String =
      lous.collect{case LocalUnit(lurn,_,ern,_,name,_,address1,_,_,_,_,postcode,sic07,employees) => lurn}.head

    Seq[LocalUnit]()
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
  
  

}
