package dao.parquet

import model.domain.{Enterprise, HBaseCell, HBaseRow}

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
      ents.collect{case Enterprise(ern,_,Some(`name`),_,_,_,_) => ern}.head

      Seq(
        Enterprise(getKeyByName("MERCATURA INVESTMENTS LIMITED") ,Some("9999999999"),Some("MERCATURA INVESTMENTS LIMITED"),Some("FS20 3OS"),Some("6"),Some("8"),Some("10")),
        Enterprise(getKeyByName("JETMORE DEVELOPMENTS LIMITED") ,Some("9999999999"),Some("JETMORE DEVELOPMENTS LIMITED"),Some("OK16 5XQ"),Some("4"),Some("4"), None),
        Enterprise(getKeyByName("5TH PROPERTY TRADING LIMITED") ,Some("9999999999"),Some("5TH PROPERTY TRADING LIMITED"),Some("HQ92 3GV"),Some("3"),Some("2"),Some("4")),
        Enterprise(getKeyByName("NBD CONTRACTS LIMITED") ,Some("9999999999"),Some("NBD CONTRACTS LIMITED"),Some("UT10 7BS"),Some("5"),Some("5"),Some("2")),
        Enterprise(getKeyByName("PURPLE SKY FUTURES LTD") ,Some("9999999999"),Some("PURPLE SKY FUTURES LTD"),Some("HG33 4OY"),Some("7"),None,None),
        Enterprise(getKeyByName("ACCLAIMED HOMES LIMITED") ,Some("9999999999"),Some("ACCLAIMED HOMES LIMITED"),Some("LB07 6UT"),Some("3"),None,None),
        Enterprise(getKeyByName("RALPH GROUP LIMITED") ,Some("9999999999"),Some("RALPH GROUP LIMITED"),Some("SI83 9RT"),Some("6"),None,None),
        Enterprise(getKeyByName("BCM TRADING LIMITED") ,Some("9999999999"),Some("BCM TRADING LIMITED"),Some("RM91 8SJ"),Some("1"),Some("6"),Some("8")),
        Enterprise(getKeyByName("GREAT GLEN CONSULTING LTD") ,Some("9999999999"),Some("GREAT GLEN CONSULTING LTD"),Some("MA61 3KB"),Some("7"),None,None),
        Enterprise(getKeyByName("TORUS DEVELOPMENT CONSULTANTS LIMITED") ,Some("9999999999"),Some("TORUS DEVELOPMENT CONSULTANTS LIMITED"),Some("FM25 8QP"),Some("7"),None,None)
    )
  }

  def testEnterprises3Recs(ents:Seq[Enterprise] ) = {

    def getKeyByName(name:String): String =
      ents.collect{case Enterprise(ern,_,Some(`name`),_,_,_,_) => ern}.head

    Seq(
      Enterprise(getKeyByName("MERCATURA INVESTMENTS LIMITED") ,Some("9999999999"),Some("MERCATURA INVESTMENTS LIMITED"),Some("FS20 3OS"),Some("6"),Some("8"),Some("10")),
      Enterprise(getKeyByName("ACCLAIMED HOMES LIMITED") ,Some("9999999999"),Some("ACCLAIMED HOMES LIMITED"),Some("LB07 6UT"),Some("3"),None,None),
      Enterprise(getKeyByName("5TH PROPERTY TRADING LIMITED") ,Some("9999999999"),Some("5TH PROPERTY TRADING LIMITED"),Some("HQ92 3GV"),Some("3"),Some("2"),Some("4"))
    )
  }



  def testLinkRowsSmallWithNullValues(entLinks:Seq[HBaseRow]) = List(
                HBaseRow("27280354~CH~201802",List(HBaseCell("p_LEU","10544190"))),
                HBaseRow("12345W~PAYE~201802",List(HBaseCell("p_LEU","15931638"))),
                HBaseRow("15931638~LEU~201802",List(HBaseCell("p_ENT","4KiXL1hlDH3GGnbbKq"), HBaseCell("c_12345W","PAYE"), HBaseCell("c_10002","VAT"), HBaseCell("c_SZ124306","CH"), HBaseCell("c_ERT12","PAYE"))),
                HBaseRow("56327266~LEU~201802",List(HBaseCell("c_10008","VAT"), HBaseCell("p_ENT","OXzCYJKOjGxLjMlORp"), HBaseCell("c_DV823168","CH"), HBaseCell("c_20016","PAYE"))),
                HBaseRow("5w8571akKLNLG9w5nK~ENT~201802",List(HBaseCell("c_60899120","LEU"))),
                HBaseRow("21840175~LEU~201802",List(HBaseCell("c_10000","VAT"), HBaseCell("p_ENT","IkFphXcsrZhdiPnHK5"), HBaseCell("c_20000","VAT"))),
                HBaseRow("JK682461~CH~201802",List(HBaseCell("p_LEU","60899120"))),
                HBaseRow("28919372~LEU~201802",List(HBaseCell("c_20002","PAYE"), HBaseCell("c_30003","PAYE"), HBaseCell("p_ENT","NcIYq7IymWXbAXckWL"))),
                HBaseRow("74515246~CH~201802",List(HBaseCell("p_LEU","99433188"))),
                HBaseRow("ERT12~PAYE~201802",List(HBaseCell("p_LEU","15931638"))),
                HBaseRow("IkFphXcsrZhdiPnHK5~ENT~201802",List(HBaseCell("c_21840175","LEU"))),
                HBaseRow("10000~VAT~201802",List(HBaseCell("p_LEU","21840175"))),
                HBaseRow("xhDzhsZ4HW9piViu7K~ENT~201802",List(HBaseCell("c_99433188","LEU"))),
                HBaseRow("69016123~LEU~201802",List(HBaseCell("p_ENT","DgfU5FlZVqlfnmO5vL"), HBaseCell("c_20010","PAYE"), HBaseCell("c_53806114","CH"), HBaseCell("c_10005","VAT"))),
                HBaseRow("30003~PAYE~201802",List(HBaseCell("p_LEU","28919372"))),
                HBaseRow("10002~VAT~201802",List(HBaseCell("p_LEU","15931638"))),
                HBaseRow("TT301000~CH~201802",List(HBaseCell("p_LEU","86883196"))),
                HBaseRow("10003~VAT~201802",List(HBaseCell("p_LEU","38557538"))),
                HBaseRow("FITAfXVNPZO8WPpbBp~ENT~201802",List(HBaseCell("c_10544190","LEU"))),
                HBaseRow("10004~VAT~201802",List(HBaseCell("p_LEU","60899120"))),
                HBaseRow("DV823168~CH~201802",List(HBaseCell("p_LEU","56327266"))),
                HBaseRow("10005~VAT~201802",List(HBaseCell("p_LEU","69016123"))),
                HBaseRow("bUKhGsRXfNsr5noO1n~ENT~201802",List(HBaseCell("c_86883196","LEU"))),
                HBaseRow("SZ124306~CH~201802",List(HBaseCell("p_LEU","15931638"))),
                HBaseRow("20018~PAYE~201802",List(HBaseCell("p_LEU","10544190"))),
                HBaseRow("20016~PAYE~201802",List(HBaseCell("p_LEU","56327266"))),
                HBaseRow("10006~VAT~201802",List(HBaseCell("p_LEU","86883196"))),
                HBaseRow("20010~PAYE~201802",List(HBaseCell("p_LEU","69016123"))),
                HBaseRow("20012~PAYE~201802",List(HBaseCell("p_LEU","86883196"))),
                HBaseRow("OXzCYJKOjGxLjMlORp~ENT~201802",List(HBaseCell("c_56327266","LEU"))),
                HBaseRow("38557538~LEU~201802",List(HBaseCell("p_ENT","mw5OqAt4ftPlwVyTSm"), HBaseCell("c_20006","PAYE"), HBaseCell("c_10003","VAT"), HBaseCell("c_09432504","CH"))),
                HBaseRow("09432504~CH~201802",List(HBaseCell("p_LEU","38557538"))),
                HBaseRow("4KiXL1hlDH3GGnbbKq~ENT~201802",List(HBaseCell("c_15931638","LEU"))),
                HBaseRow("DgfU5FlZVqlfnmO5vL~ENT~201802",List(HBaseCell("c_69016123","LEU"))),
                HBaseRow("20000~VAT~201802",List(HBaseCell("p_LEU","21840175"))),
                HBaseRow("10008~VAT~201802",List(HBaseCell("p_LEU","56327266"))),
                HBaseRow("NcIYq7IymWXbAXckWL~ENT~201802",List(HBaseCell("c_28919372","LEU"))),
                HBaseRow("86883196~LEU~201802",List(HBaseCell("c_10006","VAT"), HBaseCell("c_TT301000","CH"), HBaseCell("c_20012","PAYE"), HBaseCell("p_ENT","bUKhGsRXfNsr5noO1n"))),
                HBaseRow("53806114~CH~201802",List(HBaseCell("p_LEU","69016123"))),
                HBaseRow("99433188~LEU~201802",List(HBaseCell("c_74515246","CH"), HBaseCell("p_ENT","xhDzhsZ4HW9piViu7K"))),
                HBaseRow("60899120~LEU~201802",List(HBaseCell("c_JK682461","CH"), HBaseCell("c_20008","PAYE"), HBaseCell("c_10004","VAT"), HBaseCell("p_ENT","5w8571akKLNLG9w5nK"))),
                HBaseRow("10009~VAT~201802",List(HBaseCell("p_LEU","10544190"))),
                HBaseRow("10544190~LEU~201802",List(HBaseCell("c_10009","VAT"), HBaseCell("p_ENT","FITAfXVNPZO8WPpbBp"), HBaseCell("c_27280354","CH"), HBaseCell("c_20018","PAYE"))),
                HBaseRow("mw5OqAt4ftPlwVyTSm~ENT~201802",List(HBaseCell("c_38557538","LEU"))),
                HBaseRow("20008~PAYE~201802",List(HBaseCell("p_LEU","60899120"))),
                HBaseRow("20002~PAYE~201802",List(HBaseCell("p_LEU","28919372"))),
                HBaseRow("20006~PAYE~201802",List(HBaseCell("p_LEU","38557538")))
             ).sortBy(_.key)


  val testLinkRows3Recs = List(
    HBaseRow("testEnterpriseId-22222",List(HBaseCell("c_15931638","LEU"))),
    HBaseRow("21840175~LEU~201802",List(HBaseCell("c_10000","VAT"),HBaseCell("c_20000","VAT"),  HBaseCell("p_ENT","testEnterpriseId-11111"))),
    HBaseRow("testEnterpriseId-11111",List(HBaseCell("c_21840175","LEU"))),
    HBaseRow("testEnterpriseId-33333",List(HBaseCell("c_28919372","LEU"))),
    HBaseRow("15931638~LEU~201802",List(HBaseCell("c_10002","VAT"), HBaseCell("c_12345W","PAYE"), HBaseCell("c_ERT12","PAYE"), HBaseCell("c_SZ124306","CH"), HBaseCell("p_ENT","testEnterpriseId-22222"))),
    HBaseRow("28919372~LEU~201802",List(HBaseCell("c_20002","PAYE"), HBaseCell("c_30003","PAYE") , HBaseCell("p_ENT","testEnterpriseId-33333"))),
    HBaseRow("12345W~PAYE~201802",List(HBaseCell("p_LEU","15931638"))),
    HBaseRow("10002~VAT~201802",List(HBaseCell("p_LEU","15931638"))),
    HBaseRow("10000~VAT~201802",List(HBaseCell("p_LEU","21840175"))),
    HBaseRow("20000~VAT~201802",List(HBaseCell("p_LEU","21840175"))),
    HBaseRow("30003~PAYE~201802",List(HBaseCell("p_LEU","28919372"))),
    HBaseRow("SZ124306~CH~201802",List(HBaseCell("p_LEU","15931638"))),
    HBaseRow("20002~PAYE~201802",List(HBaseCell("p_LEU","28919372"))),
    HBaseRow("ERT12~PAYE~201802",List(HBaseCell("p_LEU","15931638")))


)//.sortBy((_.cells.map(_.column).mkString))
  
  

}
