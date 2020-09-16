import java.util.concurrent._
import scala.util.DynamicVariable

import org.scalameter._

package object scalashop extends BoxBlurKernelInterface {

  /** The value of every pixel is represented as a 32 bit integer. */
  type RGBA = Int

  /** Returns the red component. */
  def red(c: RGBA): Int = (0xff000000 & c) >>> 24

  /** Returns the green component. */
  def green(c: RGBA): Int = (0x00ff0000 & c) >>> 16

  /** Returns the blue component. */
  def blue(c: RGBA): Int = (0x0000ff00 & c) >>> 8

  /** Returns the alpha component. */
  def alpha(c: RGBA): Int = (0x000000ff & c) >>> 0

  /** Used to create an RGBA value from separate components. */
  def rgba(r: Int, g: Int, b: Int, a: Int): RGBA = {
    (r << 24) | (g << 16) | (b << 8) | (a << 0)
  }

  /** Restricts the integer into the specified range. */
  def clamp(v: Int, min: Int, max: Int): Int = {
    if (v < min) min
    else if (v > max) max
    else v
  }

  /** Image is a two-dimensional matrix of pixel values. */
  class Img(val width: Int, val height: Int, private val data: Array[RGBA]) {
    def this(w: Int, h: Int) = this(w, h, new Array(w * h))

    def apply(x: Int, y: Int): RGBA = data(y * width + x)

    def update(x: Int, y: Int, c: RGBA): Unit = data(y * width + x) = c
  }

  /** Computes the blurred RGBA value of a single pixel of the input image. */
  def boxBlurKernel(src: Img, x: Int, y: Int, radius: Int): RGBA = {
    //    var x1 = clamp(x - radius, 0, src.width - 1)
    //    val x2 = clamp(x + radius, 0, src.width - 1)
    //    val y1 = clamp(y - radius, 0, src.height - 1)
    //    val y2 = clamp(y + radius, 0, src.height - 1)
    //    var count = 0
    //    var r, g, b, a = 0: Int
    //
    //    while (x1 <= x2) {
    //      var tmpY = y1
    //      while (tmpY <= y2) {
    //        val img = src.apply(x1, tmpY)
    //        r = r + red(img)
    //        g = g + green(img)
    //        b = b + blue(img)
    //        a = a + alpha(img)
    //        tmpY += 1
    //        count += 1
    //      }
    //      x1 += 1
    //    }
    //
    //    val avgR = r / count
    //    val avgG = g / count
    //    val avgB = b / count
    //    val avgA = a / count
    //    rgba(avgR, avgG, avgB, avgA)

    val box: IndexedSeq[RGBA] = for {
      i <- clamp(x - radius, 0, x - radius) to clamp(x + radius, x, src.width - 1)
      j <- clamp(y - radius, 0, y - radius) to clamp(y + radius, y, src.height - 1)
    } yield src(i, j)
    val sum: (Int, Int, Int, Int) = box.foldRight((0, 0, 0, 0)) { (i, acc) =>
      (acc._1 + red(i), acc._2 + green(i), acc._3 + blue(i), acc._4 + alpha(i))
    }
    rgba(sum._1 / box.size,
      sum._2 / box.size,
      sum._3 / box.size,
      sum._4 / box.size)
  }

  val forkJoinPool = new ForkJoinPool

  abstract class TaskScheduler {
    def schedule[T](body: => T): ForkJoinTask[T]

    def parallel[A, B](taskA: => A, taskB: => B): (A, B) = {
      val right = task {
        taskB
      }
      val left = taskA
      (left, right.join())
    }
  }

  class DefaultTaskScheduler extends TaskScheduler {
    def schedule[T](body: => T): ForkJoinTask[T] = {
      val t = new RecursiveTask[T] {
        def compute = body
      }
      Thread.currentThread match {
        case wt: ForkJoinWorkerThread =>
          t.fork()
        case _ =>
          forkJoinPool.execute(t)
      }
      t
    }
  }

  val scheduler =
    new DynamicVariable[TaskScheduler](new DefaultTaskScheduler)

  def task[T](body: => T): ForkJoinTask[T] = {
    scheduler.value.schedule(body)
  }

  def parallel[A, B](taskA: => A, taskB: => B): (A, B) = {
    scheduler.value.parallel(taskA, taskB)
  }

  def parallel[A, B, C, D](taskA: => A, taskB: => B, taskC: => C, taskD: => D): (A, B, C, D) = {
    val ta = task {
      taskA
    }
    val tb = task {
      taskB
    }
    val tc = task {
      taskC
    }
    val td = taskD
    (ta.join(), tb.join(), tc.join(), td)
  }

  // Workaround Dotty's handling of the existential type KeyValue
  implicit def keyValueCoerce[T](kv: (Key[T], T)): KeyValue = {
    kv.asInstanceOf[KeyValue]
  }
}
