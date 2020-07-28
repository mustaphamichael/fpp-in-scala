package week1

object RecFun extends RecFunInterface {

  def main(args: Array[String]): Unit = {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(s"${pascal(col, row)} ")
      println()
    }
  }

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int =
    if (c == r || r == 0 || c <= 0) 1 else pascal(c - 1, r - 1) + pascal(c, r - 1)

  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = {
    @annotation.tailrec
    def loop(as: List[Char], count: Int, state: Boolean): Boolean =
      as match {
        case Nil => state
        case x :: xs if x == '(' => loop(xs, count + 1, state = false)
        case x :: xs if x == ')' && count > 0 && !state => loop(xs, count - 1, state = true)
        case _ :: xs => loop(xs, count, state)
      }

    loop(chars, 0, state = false)
  }

  /**
   * Exercise 3
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
}
