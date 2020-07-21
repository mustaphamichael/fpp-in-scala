def isSafe(col: Int, queens: List[Int]): Boolean = {
  val row = queens.length
  val queenWithRow = (row - 1 to 0 by -1) zip queens
  queenWithRow forall {
    case (r, c) => col != c && math.abs(col - c) != row - r
  }
}

def nQueen(n: Int): Set[List[Int]] = {
  def placeQueen(k: Int): Set[List[Int]] =
    if (k == 0) Set(List())
    else
      for {
        queens <- placeQueen(k - 1)
        col <- 0 until n
        if isSafe(col, queens)
      } yield col :: queens

  placeQueen(n)
}

// Print the chess board
def show(queens: List[Int]) = {
  val lines =
    for (col <- queens.reverse)
      yield Vector.fill(queens.length)("* ").updated(col, "X ").mkString

  "\n" + (lines mkString "\n")
}

nQueen(4) map show
