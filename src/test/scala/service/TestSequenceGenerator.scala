package service

import org.scalatest._
import util.SequenceGenerator

class TestSequenceGenerator extends FunSuite with Matchers with BeforeAndAfterAll {

  test("Generate next Sequence Number") {
    val lastSequence: Long = SequenceGenerator.currentSequence.toLong + 1
    val nextSequence: Long = SequenceGenerator.nextSequence.toLong
    assert(lastSequence === nextSequence)
    Console.out.println(s"Next Sequence Number: $nextSequence")
  }

}

