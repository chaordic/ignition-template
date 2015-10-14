package ignition.jobs

import org.apache.spark.rdd.RDD

object WordCountJob {

  val wordMatcher = "[\\p{L}'-]+".r // will match any valid unicode-words, ignoring numbers, punctuation, etc

  // This returns a RDD where each key is a word and the value is how many times it appeared in the content of lines
  def wc(lines: RDD[String]): RDD[(String, Int)] = {

    // Note: we are doing this in many lines of code for educational purposes
    // It can easily be done in a single statement

    // Create a RDD where each element is a lower-case word
    val words: RDD[String] = lines
      .flatMap(line => wordMatcher.findAllIn(line).map(_.toLowerCase()))

    // Create a pair RDD with each word as key and the initial count as value
    val mappedByWord: RDD[(String, Int)] = words
      .map(word => (word, 1)) // You can also create a tuple using the word -> 1 syntax

    // Create a RDD grouping by word and adding the counts
    val wordsWithCounts: RDD[(String, Int)] = mappedByWord
      .reduceByKey((count1, count2) => count1 + count2)


    wordsWithCounts
  }
}
