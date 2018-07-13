package model.domain

/**
  *
  */
case class VatData()

case class PayeData()
//{"id": 100000246017, "BusinessName": "BLACKWELLGROUP LTD", "UPRN": 904240, "PostCode": "CO6 2JX", "IndustryCode": "90481", "LegalStatus": "1", "TradingStatus": "B", "Turnover": "B", "EmploymentBands": "N", "VatRefs": [111222333], "PayeRefs": ["1152L", "1153L"], "CompanyNo": "00032262"}
case class LegalUnit(
                       id:String,
                       businessName:String,
                       postCode:String,
                       industryCode:String,
                       uprn:Option[String],
                       legalStatus:Option[String],
                       tradingStatus:Option[String],
                       turnover:Option[String],
                       employmentBands:Option[String],
                       vatRefs: Seq[String],
                       payeRefs: Seq[String],
                       companyNo:Option[String]
                    )

case class LegalUnitWithCalculations(
                                id:String,
                                businessName:String,
                                postCode:String,
                                industryCode:String,
                                uprn:Option[String],
                                legalStatus:Option[String],
                                tradingStatus:Option[String],
                                turnover:Option[String],
                                employmentBands:Option[String],
                                vatRefs: Seq[String],
                                payeRefs: Seq[String],
                                companyNo:Option[String],

                                payeEmployees:Option[String],
                                payeJobs:Option[String],
                                apportionedTurnover:Option[String],
                                totalTurnover:Option[String],
                                containedTurnover:Option[String],
                                standardTurnover:Option[String],
                                groupTurnover:Option[String]
                              )
object CalculatedLegalUnit{

  def apply(lu:LegalUnit) = new LegalUnitWithCalculations(
                                                    lu.id,
                                                    lu.businessName,
                                                    lu.postCode,
                                                    lu.industryCode,
                                                    lu.uprn,
                                                    lu.legalStatus,
                                                    lu.tradingStatus,
                                                    lu.turnover,
                                                    lu.employmentBands,
                                                    lu.vatRefs,
                                                    lu.payeRefs,
                                                    lu.companyNo,
                               None,None,None,None,None,None,None)

  def apply(    lu:LegalUnit,
                payeEmployees:Option[String],
                payeJobs:Option[String],
                apportionedTurnover:Option[String],
                totalTurnover:Option[String],
                containedTurnover:Option[String],
                standardTurnover:Option[String],
                groupTurnover:Option[String]
           ) = new LegalUnitWithCalculations(
                                              lu.id,
                                              lu.businessName,
                                              lu.postCode,
                                              lu.industryCode,
                                              lu.uprn,
                                              lu.legalStatus,
                                              lu.tradingStatus,
                                              lu.turnover,
                                              lu.employmentBands,
                                              lu.vatRefs,
                                              lu.payeRefs,
                                              lu.companyNo,
                                              payeEmployees,
                                              payeJobs,
                                              apportionedTurnover,
                                              totalTurnover,
                                              containedTurnover,
                                              standardTurnover,
                                              groupTurnover
                                       )

}