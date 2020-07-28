package week4

import org.junit.Assert.assertEquals
import org.junit._

class HuffmanSuite {

  import Huffman._

  trait TestTrees {
    val t1 = Fork(Leaf('a', 2), Leaf('b', 3), List('a', 'b'), 5)
    val t2 = Fork(Fork(Leaf('a', 2), Leaf('b', 3), List('a', 'b'), 5), Leaf('d', 4), List('a', 'b', 'd'), 9)
  }


  @Test def `weight of a larger tree (10pts)`: Unit =
    new TestTrees {
      assertEquals(5, weight(t1))
    }

  @Test def `chars of a larger tree (10pts)`: Unit =
    new TestTrees {
      assertEquals(List('a', 'b', 'd'), chars(t2))
    }

  @Test def `(local) makeCodeTree test`: Unit = {
    val sampleTree = makeCodeTree(makeCodeTree(Leaf('x', 1), Leaf('e', 1)), Leaf('t', 2))
    assertEquals(4, weight(sampleTree))
    assertEquals(List('x', 'e', 't'), chars(sampleTree))
  }

  @Test def `string2chars hello world`: Unit =
    assertEquals(List('h', 'e', 'l', 'l', 'o', ',', ' ', 'w', 'o', 'r', 'l', 'd'), string2Chars("hello, world"))

  @Test def `(local) number of times a character occurs`: Unit =
    assertEquals(List(('a', 2), ('b', 1)), times(List('a', 'b', 'a')))


  @Test def `make ordered leaf list for some frequency table (15pts)`: Unit =
    assertEquals(List(Leaf('e', 1), Leaf('t', 2), Leaf('x', 3)), makeOrderedLeafList(List(('t', 2), ('e', 1), ('x', 3))))


  @Test def `combine of some leaf list (15pts)`: Unit = {
    val leaflist = List(Leaf('e', 1), Leaf('t', 2), Leaf('x', 4))
    assertEquals(List(Fork(Leaf('e', 1), Leaf('t', 2), List('e', 't'), 3), Leaf('x', 4)), combine(leaflist))
  }


  @Test def `(local) combine of a singleton or nil`: Unit = {
    val singleLeafList = List(Leaf('e', 1))
    assertEquals(List(), combine(List())) // Nil
    assertEquals(List(Leaf('e', 1)), combine(singleLeafList)) // Single
  }


  @Test def `(local) decode a very short text`: Unit =
    new TestTrees {
      assertEquals(List('a'), decode(t1, List(0)))
      assertEquals(List('d'), decode(t2, List(1)))
      assertEquals(List('d', 'a'), decode(t2, List(1, 0, 0)))
      assertEquals(List('a', 'd'), decode(t2, List(0, 0, 1)))
      assertEquals(List('a', 'b'), decode(t2, List(0, 0, 0, 1)))
      assertEquals(List('a', 'b', 'd'), decode(t2, List(0, 0, 0, 1, 1)))
    }


  @Test def `decode secret text (fun)`: Unit = {
    assertEquals("huffmanestcool".toList, decodedSecret)
  }

  @Test def `(local) encode a very short text`: Unit =
    new TestTrees {
      assertEquals(List(0), encode(t1)("a".toList))
      assertEquals(List(0, 1), encode(t2)("b".toList))
      assertEquals(List(1, 0, 0), encode(t2)("da".toList))
      assertEquals(List(0, 0, 0, 1, 1), encode(t2)("abd".toList))
    }

  @Test def `decode and encode a very short text should be identity (10pts)`: Unit =
    new TestTrees {
      assertEquals("ab".toList, decode(t1, encode(t1)("ab".toList)))
      assertEquals("abd".toList, decode(t2, encode(t2)("abd".toList)))
    }


  @Test def `(local) decode and encode a very short text using 'quickEncode' should be identity`: Unit =
    new TestTrees {
      assertEquals("ab".toList, decode(t1, quickEncode(t1)("ab".toList)))
    }

  @Rule def individualTestTimeout = new org.junit.rules.Timeout(10 * 1000)
}
