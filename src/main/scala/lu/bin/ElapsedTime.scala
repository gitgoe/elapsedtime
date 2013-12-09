package lu.bin

import scala.io.Source
import java.io.File
import concurrent.Future
import concurrent.future
import concurrent.ExecutionContext.Implicits.global
import scala.util.Failure
import scala.util.Success

/**
 * @author goe
 *
 */
object ElapsedTime { 

  val DEFAULT_ENCODING = "UTF-8"
  val tagsURLLucene = """url:(.*?)json?""".r
  val tagsURLGet = """GET(.*?)-""".r
  val tagsURLPost = """POST(.*?)-""".r
  val tagsElapsed = """elapsed(.+?)ms""".r
  val tagsTime = """time(.+?)ms""".r
  val mixElapsedTime = ("(?:" + tagsElapsed.pattern.pattern + ")|(?:" + tagsTime.pattern.pattern + ")").r

  def hashKey(key: String): Option[String] = {
    if (key.contains("?")) {
      val tokens = key.substring(0, key.indexOf("?")).split("""/""").toList.tail
      //println(tokens.length)
      tokens.length match {
        case x: Int => if (x > 1) Some(tokens.head + "." + tokens
          .tail.head)
        else None
      }
    } else {
      val tokens = key.split("""/""").toList.tail
      //println(tokens.length)
      tokens.length match {
        case x: Int => if (x > 1) Some(tokens.head + "." + tokens
          .tail.head)
        else None
      }
    }
  }

  def string2Long(s: String): Long = {
    val longVal = s.replaceAll("""[^0-9]""", "").length()
    longVal match {
      case 0 => 0L
      case _ => longVal.toLong
    }
  }

  def parseLine(line: String) = line match {
    case x if x.contains("GET") => for ( tagsURLGet(url) <- tagsURLGet findFirstIn line; tagsElapsed(elapsed) <- tagsElapsed findFirstIn line ) yield (hashKey(url).getOrElse("NA"), "GET", url, string2Long(elapsed))
    case x if x.contains("POST") => for ( tagsURLPost(url) <- tagsURLPost findFirstIn line; tagsTime(elapsed) <- tagsTime findFirstIn line ) yield (hashKey(url).getOrElse("NA"), "POST", url, string2Long(elapsed))
    case _ => for ( tagsURLLucene(url) <- tagsURLLucene findFirstIn line; mixElapsedTime(elapsed) <- mixElapsedTime findFirstIn line ) yield (hashKey( url).getOrElse("NA"), "GET", url, string2Long(elapsed))
  }

  def count(input: List[(String, String, String, Long)]) = {
    input.groupBy(_._1).mapValues(_.size).map(x => (x._1, x._2))
  }

  def max(input: List[(String, String, String, Long)]) = {
    input.groupBy(_._1).values.map(_.reduceLeft((x, y) => if (x._4 > y._4)
      x else y)).map(z => (z._1, z._4))
  }

  def min(input: List[(String, String, String, Long)]) = {
    input.groupBy(_._1).values.map(_.reduceLeft((x, y) => if (x._4 < y._4)
      x else y)).map(z => (z._1, z._4))
  }

  def totalElapsedTime(input: List[(String, String, String, Long)]) = {
    input.map(x => x._4).sum
  }

  def percent(value: Long, maxValue: Long) = {
    ("%3.2f" format (value.toDouble / maxValue.toDouble) * 100) + "%"
  }

  def flatten[A, B, C, D, E, F](t: ((A, B), ((C, D), (E, F)))) = (t._1._1, t._1._2, t._2._1._2, t._2._2._2)

  def format[A, B, C, D](maxValue: Long, t: (A, Int, C, D)) = ("URL=" + t._1 + " hits=" + t._2 + "  min=" + t._3 + "ms max=" + t._4 + "ms " + percent(t._2, maxValue))

  def main(args: Array[String]): Unit = {
    println("call ElapsedTime...")

    val l1 = for (
      file <- new File("""./logs/""").listFiles.filter(_.getName().contains(".log"));
      line <- Source.fromFile(file, DEFAULT_ENCODING).getLines.filter(_.contains("elapsed"));
      link <- { /*println(line);*/ parseLine(line) }
    ) yield link

    //l1.foreach(println)

    val mCount = count(l1.toList).toList
    val mMax = max(l1.toList)
    val mMin = min(l1.toList)

    println("Total Hits=" + l1.length)
    println("Total Type URL=" + mCount.length)
    println("Total Elapsed Time=" + totalElapsedTime(l1.toList))

    val combine = (mCount zip (mMin zip mMax)).map(x => flatten(x)).sortBy(-_._2).map(y => format(l1.length, y))

    combine.foreach(println)

    println("exit ElapsedTime...")
  }

}
