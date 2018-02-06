case class Test(name:String, age:Int)

val a = Array(Test("A",10),Test("B",12),Test("V",14),Test("V",16))
val res = a.map{case t@Test("B",_) => t.copy(age=33)
case t@_ => t
}