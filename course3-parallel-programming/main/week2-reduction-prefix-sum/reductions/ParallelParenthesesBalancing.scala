package reductions

import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    @annotation.tailrec
    def loop(as: List[Char], count: Int): Int =
      as match {
        case Nil => count
        case x :: xs if x == '(' => loop(xs, count + 1)
        case x :: xs if x == ')' && count > 0 => loop(xs, count - 1)
        case x :: xs if x == ')' => loop(xs, count - 2)
        case _ :: xs => loop(xs, count)
      }

    loop(chars.toList, 0) == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    @scala.annotation.tailrec
    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): Int = {
      if (idx == until) arg1 - arg2
      else if (chars(idx) == '(') traverse(idx + 1, until, arg1 + 1, arg2)
      else if (chars(idx) == ')' && arg1 > 0) traverse(idx + 1, until, arg1, arg2 + 1)
      else if (chars(idx) == ')') traverse(idx + 1, until, arg1, arg2 - 1)
      else traverse(idx + 1, until, arg1, arg2)
    }

    def reduce(from: Int, until: Int): Int = {
      if (until - from <= threshold) traverse(from, until, 0, 0) // sequential operation
      else {
        val mid = from + (until - from) / 2
        val (c1, c2) = parallel(reduce(from, mid), reduce(mid, until))
        c1 + c2
      }
    }

    reduce(0, chars.length) == 0
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
