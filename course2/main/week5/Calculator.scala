package calculator

sealed abstract class Expr

final case class Literal(v: Double) extends Expr

final case class Ref(name: String) extends Expr

final case class Plus(a: Expr, b: Expr) extends Expr

final case class Minus(a: Expr, b: Expr) extends Expr

final case class Times(a: Expr, b: Expr) extends Expr

final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator extends CalculatorInterface {
  def computeValues(namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {
    for ((k, v) <- namedExpressions) yield k -> Signal(eval(v(), namedExpressions))
  }

  def eval(expr: Expr, references: Map[String, Signal[Expr]]): Double = {
    def parse(e: Expr): Double = e match {
      case Literal(v) => v
      case Ref(name) => eval(getReferenceExpr(name, references), references.removed(name))
      case Plus(a, b) => parse(a) + parse(b)
      case Minus(a, b) => parse(a) - parse(b)
      case Times(a, b) => parse(a) * parse(b)
      case Divide(a, b) => parse(a) / parse(b)
      case _ => Double.NaN
    }

    parse(expr)
  }

  /** Get the Expr for a referenced variables.
   * If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String, references: Map[String, Signal[Expr]]) = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
