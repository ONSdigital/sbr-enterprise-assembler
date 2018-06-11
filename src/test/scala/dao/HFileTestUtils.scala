package dao



import model.domain._

import scala.util.Try
/**
  *
  */
trait HFileTestUtils {

   def assignStaticErns(rows:Seq[HFileRow]): Set[HFileRow] = {


    //dictionary mapping actual erns to static
    val ernsDictionary: Seq[(String, String)] = {

      val erns: Seq[(String, Int)] = rows.collect{case row if(row.cells.find(_.column=="p_ENT").isDefined) => {row.cells.collect{case KVCell("p_ENT",value) => value}}}.flatten.zipWithIndex

      erns.map(ernTup => {
        val (ern,index) = ernTup
        (ern,"testEnterpriseId-"+({index+1}.toString*5))

        })}



    val lurnsDictionary: Seq[(String, String)] = {
      val lurns: Seq[(String, Int)] = rows.collect{case row if(row.cells.find(_.value=="LOU").isDefined) => {row.cells.collect{case KVCell(lurn,"LOU") => lurn}}}.flatten.zipWithIndex

     lurns.map(lurnTup => {
        val (lurn,index) = lurnTup
        (lurn.replace("c_",""),"testLocalUnitId-"+({index+1}.toString*5))

        })}


     def replaceInKey(key:String) = {
       val entityType = key.split("~")(1)

       val id = key.split(s"~$entityType~").head
       val replacementOpt  = entityType match{
         case "ENT" => ernsDictionary.find(_._1 == id).map(_._2)
         case "LOU" => lurnsDictionary.find(_._1 == id).map(_._2)
         case _ => None
       }
       replacementOpt.map(replacement => key.replace(id,replacement))
     }

     def getId(key:String) = {
         val entityTypes = Seq("ENT","LOU")
         Try{entityTypes.collect{case entityType if(key.contains(s"~$entityType~")) => key.split(s"~$entityType~").head
         }.head}.toOption

     }

     def setParentErns(cells:Iterable[KVCell[String,String]]) = cells.map(replaceParentErn)


     def replaceParentErn(cell:KVCell[String,String]) =  if (cell.column == "p_ENT") {
       val value = ernsDictionary.find(_._1 == cell.value)
       cell.copy(value = value.get._2)
     } else cell


     //replace erns in rows:
     rows.map { case row => {
       val staticKey = getId(row.key)//.flatMap(id => replaceInKey(id))
       if (staticKey.isDefined) {
         val updatedCells = row.cells.map{case cell@KVCell(col,value) => {
           val staticLurn = lurnsDictionary.find(_._1==col.replace("c_",""))
           if(value == "LOU" && staticLurn.isDefined) KVCell(staticLurn.get._2,value)
           else if(col=="p_ENT") replaceParentErn(cell)
           else cell
         }
         }
         replaceInKey(row.key).map(key => HFileRow(key, updatedCells)).getOrElse(HFileRow(staticKey.get,updatedCells))

       } else if (row.cells.find(cell => cell.column == "p_ENT").isDefined) {
         row.copy(row.key, setParentErns(row.cells))
       }  else row

     }
     }.toSet



   }






}
