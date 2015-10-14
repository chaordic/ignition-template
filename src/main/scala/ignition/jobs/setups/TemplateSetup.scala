package ignition.jobs.setups

import ignition.core.jobs.CoreJobRunner.RunnerContext
import ignition.core.jobs.utils.SparkContextUtils._
import org.apache.spark.rdd.RDD
// filterAndGetTextFiles requires a date extractor:
import ignition.core.jobs.utils.SimplePathDateExtractor.default

object TemplateSetup {

  def run(runnerContext: RunnerContext) {

    val sc = runnerContext.sparkContext
    val sparkConfig = runnerContext.config
    
  }
}
