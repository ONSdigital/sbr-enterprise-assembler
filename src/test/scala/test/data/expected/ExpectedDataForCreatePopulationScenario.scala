package test.data.expected

import model.domain.{Enterprise, HFileRow, KVCell, LocalUnit}

trait ExpectedDataForCreatePopulationScenario {

  val expectedLinks = List(
  HFileRow("20000~VAT~201802",List(KVCell("p_LEU","21840175"))),
  HFileRow("30003~PAYE~201802",List(KVCell("p_LEU","28919372"))),
  HFileRow("28919372~LEU~201802",List(KVCell("c_20002","PAYE"), KVCell("c_30003","PAYE"), KVCell("p_ENT","111111111-TEST-ERN"))),
  HFileRow("333333333-TEST-ERN~ENT~201802",List(KVCell("c_15931638","LEU"), KVCell("c_33333333-TEST-LURN","LOU"))),
  HFileRow("10000~VAT~201802",List(KVCell("p_LEU","21840175"))),
  HFileRow("21840175~LEU~201802",List(KVCell("c_10000","VAT"), KVCell("c_20000","VAT"), KVCell("p_ENT","222222222-TEST-ERN"))),
  HFileRow("33333333-TEST-LURN~LOU~201802",List(KVCell("p_ENT","333333333-TEST-ERN"))),
  HFileRow("11111111-TEST-LURN~LOU~201802",List(KVCell("p_ENT","111111111-TEST-ERN"))),
  HFileRow("22222222-TEST-LURN~LOU~201802",List(KVCell("p_ENT","222222222-TEST-ERN"))),
  HFileRow("111111111-TEST-ERN~ENT~201802",List(KVCell("c_11111111-TEST-LURN","LOU"), KVCell("c_28919372","LEU"))),
  HFileRow("12345W~PAYE~201802",List(KVCell("p_LEU","15931638"))),
  HFileRow("ERT12~PAYE~201802",List(KVCell("p_LEU","15931638"))),
  HFileRow("20002~PAYE~201802",List(KVCell("p_LEU","28919372"))),
  HFileRow("10002~VAT~201802",List(KVCell("p_LEU","15931638"))),
  HFileRow("15931638~LEU~201802",List(KVCell("c_12345W","PAYE"), KVCell("c_ERT12","PAYE"), KVCell("c_SZ124306","CH"), KVCell("p_ENT","333333333-TEST-ERN"), KVCell("c_10002","VAT"))),
  HFileRow("SZ124306~CH~201802",List(KVCell("p_LEU","15931638"))),
  HFileRow("222222222-TEST-ERN~ENT~201802",List(KVCell("c_21840175","LEU"), KVCell("c_22222222-TEST-LURN","LOU")))
  )

  val expectedEnts = List(

      Enterprise("111111111-TEST-ERN",Some("9999999999"),"5TH PROPERTY TRADING LIMITED","",None,None,None,None,"HQ92 3GV",None,"90481","3",Some("2"),Some("4"),None,None,None,None,None),
      Enterprise("222222222-TEST-ERN",Some("9999999999"),"ACCLAIMED HOMES LIMITED","",None,None,None,None,"LB07 6UT",None,"00742","3",None,None,None,Some("85"),Some("85"),None,None),
      Enterprise("333333333-TEST-ERN",Some("9999999999"),"MERCATURA INVESTMENTS LIMITED","",None,None,None,None,"FS20 3OS",None,"70176","6",Some("8"),Some("10"),None,Some("45"),Some("45"),None,None)
  )

  val expectedLous = List(
    new LocalUnit(
      lurn="11111111-TEST-LURN",
      luref=None,
      ern="111111111-TEST-ERN",
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
      employees = "2"
    ),
    new LocalUnit(
      lurn="22222222-TEST-LURN",
      luref=None,
      ern="222222222-TEST-ERN",
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
      lurn="33333333-TEST-LURN",
      luref=None,
      ern="333333333-TEST-ERN",
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
      employees = "8"
    )

  )
}
