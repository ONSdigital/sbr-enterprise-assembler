package utils

import model.domain.Enterprise

trait TestDataUtils extends HFileTestUtils{



  def replaceDynamiclyGeneratedErns(ents:Seq[Enterprise] ) = {

    def getKeyByName(name:String): String =
      ents.collect{case Enterprise(ern,_,`name`,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_,_) => ern}.head

    Seq(
      Enterprise(getKeyByName("MERCATURA INVESTMENTS LIMITED") ,Some("9999999999"),"MERCATURA INVESTMENTS LIMITED","",None,None,None,None,"FS20 3OS",None,"70176","6",Some("8"),Some("10"),None,Some("45"),Some("45"),None,None),
      Enterprise(getKeyByName("ACCLAIMED HOMES LIMITED") ,Some("9999999999"),"ACCLAIMED HOMES LIMITED","",None,None,None,None,"LB07 6UT",None,"00742","3",None,None,None,Some("85"),Some("85"),None,None),
      Enterprise(getKeyByName("5TH PROPERTY TRADING LIMITED") ,Some("9999999999"),"5TH PROPERTY TRADING LIMITED","",None,None,None,None,"HQ92 3GV",None,"90481","3",Some("2"), Some("4"),None,None,None,None,None)
    )
  }
}
