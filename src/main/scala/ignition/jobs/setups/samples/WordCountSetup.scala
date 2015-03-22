package ignition.jobs.setups.samples

import ignition.core.jobs.CoreJobRunner.RunnerContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


object WordCountSetup {

  val wordMatcher = "[\\p{L}'-]+".r // will match any valid unicode-words, ignoring numbers, punctuation, etc

  def run(runnerContext: RunnerContext) {

    val sc = runnerContext.sparkContext
    val sparkConfig = runnerContext.config

    // Count words and sort from highest-frequency words to lowest
    val text: RDD[String] = sc
      .textFile("s3n://mr101ufcg/data/gutenberg")

    val words: RDD[String] = text
      .flatMap(line => wordMatcher.findAllIn(line).map(_.toLowerCase()))

    val mappedByWord: RDD[(String, Int)] = words
      .map(word => (word, 1))

    val wordsWithCounts: RDD[(String, Int)] = mappedByWord
      .reduceByKey((count1, count2) => count1 + count2)

    val countsWithWords: RDD[(Int, String)] = wordsWithCounts
      .map { case (word, count) => (count, word) }


    val top1000Words: Seq[(Int, String)] = countsWithWords.top(1000)

    // print top words
    top1000Words.zipWithIndex.foreach { case ((freq, word), i) => println(s"${i+1}) $freq - '$word'") }
  }
}
