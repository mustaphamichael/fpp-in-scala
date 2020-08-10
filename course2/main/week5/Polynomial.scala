package calculator

object Polynomial extends PolynomialInterface {
  def computeDelta(a: Signal[Double], b: Signal[Double],
                   c: Signal[Double]): Signal[Double] = {
    Signal(math.pow(b(), 2) - 4 * a() * c())
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
                       c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    Signal(if (delta() < 0) Set.empty
    else {
      val plus = (-b() + math.sqrt(delta())) / (2 * a())
      val minus = (-b() - math.sqrt(delta())) / (2 * a())
      Set(plus, minus)
    })
  }
}
