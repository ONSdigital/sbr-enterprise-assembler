package sequence

import org.scalatest._
import util.SequenceGenerator
import util.options.{Config, ConfigOptions, OptionNames}

class TestSequenceGenerator extends FunSuite with Matchers with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    Config.set(OptionNames.SequenceURL, "localhost:2181")
  }

  test("Generate next Sequence Number") {
    val lastSequence: Long = Sequence.currentSequence.toLong + 1
    val nextSequence: Long = Sequence.nextSequence.toLong
    assert(lastSequence === nextSequence)
    Console.out.println(s"Next Sequence Number: $nextSequence")
  }

}

object Sequence extends SequenceGenerator(ConfigOptions.SequenceURL)