package dao.parquet

import model.Ent

/**
  *
  */
trait TestData { 

/**
  * creates Array[Ent] for matching actual results returned by HBase.
  * As ENT keys generated dynamically and cannot be matched, the keys are copied from actual results so that the rest of Ent object's attribute values
  * can be checked for equality
  * */
  def testEnterprises(ents:Seq[Ent]) = {

    def getKeyByName(name:String): String = ents.find(_.businessName.get==name).map(_.ern).getOrElse(throw new IllegalArgumentException("cannot find key"))

      Array(
      Ent(getKeyByName("MERCATURA INVESTMENTS LIMITED") ,Some("9999999999"),Some("MERCATURA INVESTMENTS LIMITED"),Some("FS20 3OS"),Some("6")),
      Ent(getKeyByName("JETMORE DEVELOPMENTS LIMITED") ,Some("9999999999"),Some("JETMORE DEVELOPMENTS LIMITED"),Some("OK16 5XQ"),Some("4")),
      Ent(getKeyByName("5TH PROPERTY TRADING LIMITED") ,Some("9999999999"),Some("5TH PROPERTY TRADING LIMITED"),Some("HQ92 3GV"),Some("3")),
      Ent(getKeyByName("NBD CONTRACTS LIMITED") ,Some("9999999999"),Some("NBD CONTRACTS LIMITED"),Some("UT10 7BS"),Some("5")),
      Ent(getKeyByName("PURPLE SKY FUTURES LTD") ,Some("9999999999"),Some("PURPLE SKY FUTURES LTD"),Some("HG33 4OY"),Some("7")),
      Ent(getKeyByName("ACCLAIMED HOMES LIMITED") ,Some("9999999999"),Some("ACCLAIMED HOMES LIMITED"),Some("LB07 6UT"),Some("3")),
      Ent(getKeyByName("RALPH GROUP LIMITED") ,Some("9999999999"),Some("RALPH GROUP LIMITED"),Some("SI83 9RT"),Some("6")),
      Ent(getKeyByName("BCM TRADING LIMITED") ,Some("9999999999"),Some("BCM TRADING LIMITED"),Some("RM91 8SJ"),Some("1")),
      Ent(getKeyByName("GREAT GLEN CONSULTING LTD") ,Some("9999999999"),Some("GREAT GLEN CONSULTING LTD"),Some("MA61 3KB"),Some("7")),
      Ent(getKeyByName("TORUS DEVELOPMENT CONSULTANTS LIMITED") ,Some("9999999999"),Some("TORUS DEVELOPMENT CONSULTANTS LIMITED"),Some("FM25 8QP"),Some("7"))
    )
  }
}
