package utils.data.existing

import model.domain.{HFileRow, KVCell}
import utils.data.TestIds

/**
  *
  */
trait ExistingLegalUnits {this:TestIds =>

  val existingLeusForNewPeriodScenario = Seq(
    HFileRow(s"${entWithMissingLouId.reverse}~100002826247",List(KVCell("ubrn","100002826247"),KVCell("ern",entWithMissingLouId),KVCell("crn","00032262"),KVCell("name","INDUSTRIES LTD"), KVCell("trading_style","A") ,KVCell("address1","P O BOX 22"), KVCell("address2","INDUSTRIES HOUSE"), KVCell("address3","WHITE LANE"), KVCell("address4","REDDITCH"), KVCell("address5","WORCESTERSHIRE"), KVCell("postcode","B22 2TL"), KVCell("sic07","12345"), KVCell("paye_jobs","4"), KVCell("turnover","500"), KVCell("legal_status","1"), KVCell("trading_status","B"), KVCell("birth_date","01/03/2018"))),
    HFileRow("1100000003~100000246017",List(KVCell("ubrn","100000246017"),KVCell("ern","3000000011"),KVCell("crn","00032262"),KVCell("name","BLACKWELLGROUP LTD"), KVCell("trading_style","B"),KVCell("address1","GOGGESHALL ROAD"), KVCell("address2","EARLS COLNE"), KVCell("address3","COLCHESTER"), KVCell("postcode","CO6 2JX"), KVCell("sic07","23456"), KVCell("paye_jobs","2"), KVCell("turnover","200"), KVCell("legal_status","1"), KVCell("trading_status","A"), KVCell("birth_date","02/042017"))),
    HFileRow("1100000003~100000827984",List(KVCell("ubrn","100000827984"),KVCell("ern","3000000011"),KVCell("crn","100000827984"),KVCell("name","2-ND LU OF BLACKWELLGROUP LTD"), KVCell("trading_style","B"),KVCell("address1","North End Rd lane"), KVCell("address2","Croydon"), KVCell("address3","Surrey"), KVCell("postcode","CR0 1AA"), KVCell("sic07","1122"), KVCell("paye_jobs","2"), KVCell("legal_status","1"), KVCell("trading_status","C"), KVCell("birth_date","03/05/2016"))),
    HFileRow("1100000004~100000459235",List(KVCell("ubrn","100000459235"),KVCell("ern","4000000011"),KVCell("crn","04223164"),KVCell("name","IBM LTD"), KVCell("trading_style","C"),KVCell("address1","BSTER DEPT"), KVCell("address2","MAILPOINT A1F"), KVCell("address3","P O BOX 41"), KVCell("address4","NORTH HARBOUR"), KVCell("address5","PORTSMOUTH"), KVCell("postcode","CF23 9EU"), KVCell("sic07","3344"), KVCell("paye_jobs","2"), KVCell("legal_status","1"), KVCell("trading_status","A"), KVCell("birth_date","04/06/2015"))),
    HFileRow("1100000004~100000508723",List(KVCell("ubrn","100000508723"),KVCell("ern","4000000011"),KVCell("crn","04223165"),KVCell("name","2-ND LU OF IBM LTD"), KVCell("trading_style","A"),KVCell("address1","IT DEPT"), KVCell("address2","1 Hight Street"), KVCell("address3","Newport"), KVCell("address4","SOUTH WALES"), KVCell("postcode","NP10 6XG"), KVCell("sic07","2233"), KVCell("paye_jobs","2"), KVCell("legal_status","1"), KVCell("trading_status","B"), KVCell("birth_date","05/07/2014"))),
    HFileRow("1100000004~100000508724",List(KVCell("ubrn","100000508724"),KVCell("ern","4000000011"),KVCell("crn","00012345"),KVCell("name","3-RD LU OF IBM LTD"), KVCell("trading_style","B"),KVCell("address1","IBM HOUSE"), KVCell("address2","Smile Street"), KVCell("address3","Cardiff"), KVCell("address4","SOUTH WALES"), KVCell("postcode","CF23 9EU"), KVCell("sic07","3344"), KVCell("paye_jobs","1"), KVCell("legal_status","1"), KVCell("trading_status","C"), KVCell("birth_date","06/08/2013"))),
    HFileRow("1100000005~100000508888",List(KVCell("ubrn","100000508888"),KVCell("ern","5000000011"),KVCell("crn","00032262"),KVCell("name","MBI LTD"), KVCell("trading_style","U") ,KVCell("address1","99 Pen-Y-Lan Terrace"), KVCell("address2","Unit 11"), KVCell("address3","Cardiff"),KVCell("postcode","CF23 9EU"), KVCell("sic07","44044"), KVCell("paye_jobs","4"), KVCell("legal_status","2"), KVCell("trading_status","C"), KVCell("birth_date","06/08/2010")))
  )

}
