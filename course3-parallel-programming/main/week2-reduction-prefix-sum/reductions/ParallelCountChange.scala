package reductions

import org.scalameter._

object ParallelCountChangeRunner {

  @volatile var seqResult = 0

  @volatile var parResult = 0

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 20,
    Key.exec.maxWarmupRuns -> 40,
    Key.exec.benchRuns -> 80,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val amount = 250
    val coins = List(1, 2, 5, 10, 20, 50)
    val seqtime = standardConfig measure {
      seqResult = ParallelCountChange.countChange(amount, coins)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential count time: $seqtime")

    def measureParallelCountChange(threshold: => ParallelCountChange.Threshold): Unit = try {
      val fjtime = standardConfig measure {
        parResult = ParallelCountChange.parCountChange(amount, coins, threshold)
      }
      println(s"parallel result = $parResult")
      println(s"parallel count time: $fjtime")
      println(s"speedup: ${seqtime.value / fjtime.value}")
    } catch {
      case e: NotImplementedError =>
        println("Not implemented.")
    }

    println("\n# Using moneyThreshold\n")
    measureParallelCountChange(ParallelCountChange.moneyThreshold(amount))
    println("\n# Using totalCoinsThreshold\n")
    measureParallelCountChange(ParallelCountChange.totalCoinsThreshold(coins.length))
    println("\n# Using combinedThreshold\n")
    measureParallelCountChange(ParallelCountChange.combinedThreshold(amount, coins))
  }
}

object ParallelCountChange extends ParallelCountChangeInterface {

  /** Returns the number of ways change can be made from the specified list of
   * coins for the specified amount of money.
   */
  def countChange(money: Int, coins: List[Int]): Int = {
    def loop(result: List[Int], amount: Int, coins: List[Int]): List[List[Int]] = {
      if (amount == 0) List(result)
      else coins match {
        case Nil => List()
        case x :: xs if amount < 0 => List()
        case x :: xs => loop(x :: result, amount - x, coins) ++
          loop(result, amount, xs) // greedy algorithm + backtracking
      }
    }

    loop(List(), money, coins).size
  }

  type Threshold = (Int, List[Int]) => Boolean

  /** In parallel, counts the number of ways change can be made from the
   * specified list of coins for the specified amount of money.
   */
  def parCountChange(money: Int, coins: List[Int], threshold: Threshold): Int = {
    if (money < 0 || coins.isEmpty) 0
    else coins match {
      case x :: xs if threshold(money, coins) => countChange(money, coins)
      case x :: xs =>
        val (v1, v2) = parallel(
          parCountChange(money - x, coins, threshold),
          parCountChange(money, xs, threshold))
        v1 + v2
    }
  }

  /** Threshold heuristic based on the starting money. */
  def moneyThreshold(startingMoney: Int): Threshold =
    (money, _) => money <= (startingMoney * 2) / 3

  /** Threshold heuristic based on the total number of initial coins. */
  def totalCoinsThreshold(totalCoins: Int): Threshold =
    (_, coins) => coins.size <= (totalCoins * 2) / 3

  /** Threshold heuristic based on the starting money and the initial list of coins. */
  def combinedThreshold(startingMoney: Int, allCoins: List[Int]): Threshold = {
    (money, coins) => (money * coins.size) <= (startingMoney * allCoins.size / 2)
  }
}

// Result analysis (Intel i5, 4 cores, 12GB RAM)
// Result = 177863 (Correct)
// Sequential Time : 412.6230677750001 ms
// Parallel Time :
//  - Money Threshold : 90.77283597500004 ms ; speedup: 4.545666810373112
//  - TotalCoins Threshold: 69.30030690000002 ms ; speedup: 5.9541304538580615 (Fasted)
//  - Combined Threshold : 89.97041521249999 ms ; speedup: 4.5862083308210915