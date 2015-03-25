package ignition.jobs

import ignition.core.testsupport.spark.SharedSparkContext
import org.scalatest.{ShouldMatchers, FlatSpec}


class WordCountSpec extends FlatSpec with ShouldMatchers with SharedSparkContext {

  "WordCount" should "count words correctly" in {
    val text =
      """
        |She sells sea-shells on the sea-shore.
        |The shells she sells are sea-shells, I'm sure.
        |For if she sells sea-shells on the sea-shore
        |Then I'm sure she sells sea-shore shells.
      """.stripMargin
    val lines = sc.parallelize(text.split("\n"))
    val wordCount = WordCountJob.wc(lines)

    val expectedResut = Map(("are",1), ("sea-shells",3), ("if",1), ("on",2), ("shells",2),
      ("then",1), ("sure",2), ("she",4), ("for",1), ("i'm",2),
      ("sea-shore",3), ("sells",4), ("the",3))

    wordCount.collect().toMap shouldBe expectedResut
  }

}
