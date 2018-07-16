package model.domain

/**
  *
  */
case class PayeData(payeRef:String,q1:Option[String],q2:Option[String],q3:Option[String],q4:Option[String])
object PayeData{
  def apply(payeRef:String) = new PayeData(payeRef, None,None,None,None)
}

case class VatData(vatRef:String,turnover:Option[String],recordType:Option[String])
object VatData{
  def apply(vatRef:String) = new VatData(vatRef,None,None)
}

case class LegalUnit(
                       id:String,
                       ern:String,
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
                                ern:String,
                                businessName:String,
                                postCode:String,
                                industryCode:String,
                                uprn:Option[String],
                                legalStatus:Option[String],
                                tradingStatus:Option[String],
                                turnover:Option[String],
                                employmentBands:Option[String],
                                vatRefs: Seq[VatData],
                                payeRefs: Seq[PayeData],
                                companyNo:Option[String],

                                payeEmployees:Option[String],
                                payeJobs:Option[String],
                                apportionedTurnover:Option[String],
                                totalTurnover:Option[String],
                                containedTurnover:Option[String],
                                standardTurnover:Option[String],
                                groupTurnover:Option[String]
                              )
object LegalUnitWithCalculations{

  def apply(lu:LegalUnit) = new LegalUnitWithCalculations(
                                                    lu.id,
                                                    lu.ern,
                                                    lu.businessName,
                                                    lu.postCode,
                                                    lu.industryCode,
                                                    lu.uprn,
                                                    lu.legalStatus,
                                                    lu.tradingStatus,
                                                    lu.turnover,
                                                    lu.employmentBands,
                                                    lu.vatRefs.map(VatData(_)),
                                                    lu.payeRefs.map(PayeData(_)),
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
                                              lu.ern,
                                              lu.businessName,
                                              lu.postCode,
                                              lu.industryCode,
                                              lu.uprn,
                                              lu.legalStatus,
                                              lu.tradingStatus,
                                              lu.turnover,
                                              lu.employmentBands,
                                              lu.vatRefs.map(VatData(_)),
                                              lu.payeRefs.map(PayeData(_)),
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