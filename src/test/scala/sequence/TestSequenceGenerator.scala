package sequence

import global.Configs
import org.scalatest._
import util.SequenceGenerator

class TestSequenceGenerator extends FunSuite with Matchers with BeforeAndAfterAll {

//  override def beforeAll: Unit = {
//    new TestingServer(2181, true)
//  }

  test("Generate next Sequence Number") {
    val lastSequence: Long = Sequence.currentSequence.toLong + 1
    val nextSequence: Long = Sequence.nextSequence.toLong
    assert(lastSequence === nextSequence)
    Console.out.println(s"Next Sequence Number: $nextSequence")
  }

}

object Sequence extends SequenceGenerator(Configs.config.getString("hbase.zookeper.url"))