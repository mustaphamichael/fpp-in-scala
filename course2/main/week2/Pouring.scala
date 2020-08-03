/**
 * Week 2 Course work
 * Implementing the Water Pouring problem using FP principles (Lazy Evals & Streams)
 */
package week2

class Pouring(capacity: Vector[Int]) {
  // The Water Pouring problem

  // States
  type State = Vector[Int]

  // initial state is equal to zero (0)
  val initialState = capacity map (x => 0)

  // Moves
  trait Move {
    def change(state: State): State
  }

  case class Empty(glass: Int) extends Move {
    override def change(state: State): State = state updated(glass, 0)
  }

  case class Fill(glass: Int) extends Move {
    override def change(state: State): State = state updated(glass, capacity(glass))
  }

  case class Pour(from: Int, to: Int) extends Move {
    override def change(state: State): State = {
      val amount = state(from) min (capacity(from) - state(to)) // Ensure to analyze this
      // deduct from "from" glass and increase "to" glass
      state updated(from, state(from) - amount) updated(to, state(to) + amount)
    }
  }

  val glasses = capacity.indices

  val moves =
    (for (g <- glasses) yield Empty(g)) ++
      (for (g <- glasses) yield Fill(g)) ++
      (for (from <- glasses; to <- glasses if from != to) yield Pour(from, to))


  class Path(history: List[Move], val endState: State) {
    //    def endState: State = (history foldRight initialState) (_ change _)

    //    def endState: State = {
    //      def trackState(xs: List[Move]): State = xs match {
    //        case Nil => initialState
    //        case move :: xs1 => move change trackState(xs1)
    //      }
    //
    //      trackState(history)
    //    }


    def extend(move: Move) = new Path(move :: history, move change endState)

    override def toString: String = (history.reverse mkString (" ")) + "-->" + endState
  }

  val initialPath = new Path(Nil, initialState)

  def from(paths: Set[Path], explored: Set[State]): Stream[Set[Path]] =
    if (paths.isEmpty) Stream.empty
    else {
      val more = for {
        path <- paths
        next <- moves map (path.extend)
        if !(explored.contains(next.endState))
      } yield next
      more #:: from(more, explored ++ (more.map(_.endState)))
    }

  val pathSets = from(Set(initialPath), Set(initialState))

  def solution(target: Int): Stream[Path] =
    for {
      pathSet <- pathSets
      path <- pathSet
      if path.endState contains target
    } yield path

}

object Test extends App {
  val problem = new Pouring(Vector(4, 9))
  println(problem.solution(6))
}
