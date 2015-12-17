package ignition.jobs.setups


import ignition.jobs.WordCountJob
import org.apache.spark.rdd.RDD

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.utils.SparkContextUtils._
// filterAndGetParallelTextFiles requires a date extractor:
import ignition.core.jobs.utils.SimplePathDateExtractor.default

// Count words and give the top 1000 highest-frequency
object WordCountSetup {

  def run(runnerContext: RunnerContext) {

    val sc = runnerContext.sparkContext
    val sparkConfig = runnerContext.config

    val lines: RDD[String] = sc.filterAndGetParallelTextFiles("s3n://mr101ufcg/data/gutenberg")

    // The job is generic and can be used in other contexts
    // This setup is specific because it binds a certain source of files (gutenberg books)
    // to a certain output (top 1000 words printed in console)
    val wordCount: RDD[(String, Int)] = WordCountJob.wc(lines)

    val top1000Words: Seq[(Int, String)] = wordCount
      .map { case (word, count) => (count, word) }
      .top(1000)

    // print top words
    top1000Words.zipWithIndex.foreach { case ((freq, word), i) => println(s"${i+1}) $freq - '$word'") }
  }
}
