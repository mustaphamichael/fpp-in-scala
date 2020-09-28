package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.junit._
import org.junit.Assert.assertEquals
import java.io.File

object StackOverflowSuite {
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)
}

class StackOverflowSuite {

  import StackOverflowSuite._


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  lazy val postings = {
    val csv =
      """1,27233496,,,0,C#
        |1,23698767,,,9,C#
        |1,5484340,,,0,C#
        |2,5494879,,5484340,1,
        |1,9002525,,,2,C++
        |2,9003401,,9002525,4,
        |2,9003942,,9002525,1,
        |2,9005311,,9002525,0,""".stripMargin.split("\n")
    val rdd = sc.parallelize(csv)
    testObject.rawPostings(rdd)
  }.persist


  @Test def `testObject can be instantiated`: Unit = {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  @Test def `groupPostings should work as intended`: Unit = {
    val grouped = testObject.groupedPostings(postings)
    println(grouped.collect.toList)
    assert(grouped.collect.length == 2, "There are only two questions with answers")
  }

  @Test def `scoredPosting should return RDD(Question, HighestScore_of_Answer)`: Unit = {
    val grouped = testObject.groupedPostings(postings)
    val scoredPost = testObject.scoredPostings(grouped).collect
    assert(scoredPost(0)._2 == 1)
    assert(scoredPost(1)._2 == 4)
  }


  @Rule def individualTestTimeout = new org.junit.rules.Timeout(100 * 1000)
}
