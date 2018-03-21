package dao



import model.domain._
/**
  *
  */
trait HFileTestUtils {

   def assignStaticKeys(rows:Seq[HFileRow]): Set[HFileRow] = {

    val erns: Seq[(String, Int)] = rows.collect{case row if(row.cells.find(_.column=="p_ENT").isDefined) => {row.cells.collect{case HFileCell("p_ENT",value) => value}}}.flatten.zipWithIndex

    val ernsDictionary: Seq[(String, String)] = erns.map(ernTup => {
                                                              val (ern,index) = ernTup
                                                              (ern,"testEnterpriseId-"+({index+1}.toString*5))

                                                              })

                //replace erns in actual:
    rows.map {  case row  => {
                            if (ernsDictionary.find(_._1 == row.key.slice(0, 18)).isDefined) {
                            val ern = ernsDictionary.find(_._1 == row.key.slice(0, 18))
                            val rr: HFileRow = HFileRow(ern.get._2, row.cells)
                            rr

                          }else if(row.cells.find(cell => cell.column=="p_ENT").isDefined) {
                            row.copy(row.key, row.cells.map {cell =>
                            if (cell.column == "p_ENT"){
                            val value = ernsDictionary.find(_._1 == cell.value)
                            cell.copy(value = value.get._2)
                          }else cell
                          })
                          }else row
              }}.toSet
   }

}
