package test.data.expected

import model.domain.{HFileRow, KVCell, LocalUnit}

trait ExpectedDataForCreatePopulationScenario {

  val expectedLinks = List(

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

  val expectedLous = List(
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
      employees = "2"
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
      employees = "8"
    )

  )
}
