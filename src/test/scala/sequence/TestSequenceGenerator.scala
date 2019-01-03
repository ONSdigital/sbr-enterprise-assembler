package sequence

import org.scalatest._
import util.SequenceGenerator
import util.options.{Config, OptionNames}

class TestSequenceGenerator extends FunSuite with Matchers with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    Config.set(OptionNames.SequenceURL, "localhost:2181")
    Config.set(OptionNames.SequenceFormat, "localhost:2181")
    Config.set(OptionNames.SequencePath, "/ids/enterprise/id")
    Config.set(OptionNames.SequenceConnectionTimeout, "5")
    Config.set(OptionNames.SequenceSessionTimeout, "5")
  }

  test("Generate next Sequence Number") {
    val lastSequence: Long = SequenceGenerator.currentSequence.toLong + 1
    val nextSequence: Long = SequenceGenerator.nextSequence.toLong
    assert(lastSequence === nextSequence)
    Console.out.println(s"Next Sequence Number: $nextSequence")
  }

}

