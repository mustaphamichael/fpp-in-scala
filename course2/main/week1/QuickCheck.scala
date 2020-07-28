package quickcheck

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  private val genEmpty = const(empty)

  lazy val genHeap: Gen[H] = oneOf(
    genEmpty,
    for {
      elem <- arbitrary[A]
      node <- oneOf(genEmpty, genHeap)
    } yield insert(elem, node)
  )

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  /** Finding properties */
  // for any heap, adding the minimal element,
  // and then finding it, should return the element in question
  property("generateHeap") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  // adding a single element to an empty heap,
  // and then removing this element, should yield the element in question
  property("insertAndRemove") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  // If you insert any two elements into an empty heap,
  // finding the minimum of the resulting heap should get the smallest of the two elements back
  property("insertAndMinimum") = forAll { (v1: Int, v2: Int) =>
    val h = insert(v1, insert(v2, empty))
    findMin(h) == (v1 min v2)
  }

  // If you insert an element into an empty heap,
  // then delete the minimum, the resulting heap should be empty
  property("minimumDelete") = forAll { a: Int =>
    val h = insert(a, empty)
    deleteMin(h) == empty
  }

  // Given any heap, you should get a sorted sequence of elements when continually finding and deleting minima.
  // (Hint: recursion and helper functions are your friends.)
  property("sort") = forAll { h: H =>
    @scala.annotation.tailrec
    def deleteHeap(heap: H, acc: List[A]): List[A] = {
      if (isEmpty(heap)) acc
      else deleteHeap(deleteMin(heap), findMin(heap) :: acc)
    }

    val deleted = deleteHeap(h, List())
    deleted == deleted.sorted
  }

  // Finding a minimum of the melding of any two heaps
  // should return a minimum of one or the other

  // Generate only non-empty heads
  private val heap1 = arbitrary[H] suchThat (!isEmpty(_))
  private val heap2 = arbitrary[H] suchThat (!isEmpty(_))
  property("meld") = forAll(heap1, heap2) { (h1, h2) =>
    val meldMin = findMin(meld(h1, h2))
    meldMin == findMin(h1) || meldMin == findMin(h2)
  }
}
