val list = List(1, 2, 3, 4, 5, 6, 7)

list.updated(6, 77)

def init[T](xs: List[T]): List[T] = xs match {
  case Nil => throw new Error("init of empty list")
  case List(x) => Nil
  case y :: ys => y :: init(ys)
}

// O(n*n) - Quadratic - Very Bad
def reverse[T](xs: List[T]): List[T] = xs match {
  case Nil => Nil
  case y :: ys => reverse(ys) :+ y
}

def removeAt[T](n: Int, xs: List[T]): List[T] = {
  def loop(start: Int, arr: List[T]): List[T] = {
    arr match {
      case Nil => Nil
      case y :: ys if start == n => ys
      case y :: ys => y :: loop(start + 1, ys)
    }
  }

  loop(0, xs)
}

// Insertion sort O(n*n)
def isort(xs: List[Int]): List[Int] = {
  def insert(y: Int, ys: List[Int]): List[Int] = ys match {
    case Nil => List(y)
    case z :: zs => if (y <= z) y :: ys else z :: insert(y, zs)
  }

  xs match {
    case Nil => Nil
    case y :: ys => insert(y, isort(ys))
  }
}

// Merge Sort O(nlogn)

import scala.math.Ordering

//def msort[T](xs: List[T])(lt: (T, T) => Boolean): List[T] = {
def msort[T](xs: List[T])(implicit ord: Ordering[T]): List[T] = {
  val n = xs.length / 2
  if (n == 0) xs // length as 0 or 1 is already sorted
  else {
    def merge(ys: List[T], zs: List[T]): List[T] =
      (ys, zs) match {
        case (Nil, zs) => zs
        case (ys, Nil) => ys
        case (y :: ysT, z :: zsT) =>
          if (ord.lt(y, z)) y :: merge(ysT, zs)
          else z :: merge(ys, zsT)
      }

    val (fst, snd) = xs splitAt n
    merge(msort(fst), msort(snd))
  }
}

def map[T](xs: List[T])(f: T => T): List[T] = xs match {
  case Nil => xs
  case y :: ys => f(y) :: map(ys)(f)
}

// pack consecutive elements
def pack[T](xs: List[T]): List[List[T]] = xs match {
  case Nil => Nil
  case y :: ys =>
    val (first, rest) = xs span (k => k == y)
    first :: pack(rest)
}

// find length of list using foldRight
def lengthFun[T](xs: List[T]) =
  (xs foldRight 0)((_, y) => 1 + y)

def mapFun[T, U](xs: List[T], f: T => U): List[U] =
  (xs foldRight List[U]())((x, y) => f(x) :: y)

//init(list)
//removeAt(6, list)
//isort(List(1, 4, 8, 2, 1, 2))
//msort(List(1, 4, 8, 2, 1, 2))
//val fruits = List("mango", "apple", "ginger", "banana")
//msort(fruits)((x, y) => x.compareTo(y) < 0)
//msort(fruits)
//map(List(1, 4, 8))(x => x * x)
//List(1, 4, 8).map(math.pow(_, 2).toInt)

//pack(List("a", "a", "a", "b", "c", "c", "a"))
lengthFun(list)

mapFun(list, (x: Int) => x * x)