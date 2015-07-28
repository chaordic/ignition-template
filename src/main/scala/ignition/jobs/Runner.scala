package ignition.jobs

import ignition.core.jobs.CoreJobRunner
import ignition.jobs.setups._

object Runner {

  // Binds a setup name to a function that will run this setup and (optionally) a custom
  // extra configuration for the given setup
  val availableJobsSetups = Map[String, (CoreJobRunner.RunnerContext => Unit, Map[String, String])](
    // Simple Samples
    ("HelloWorldSetup", (context => HelloWorldSetup.run(context), Map.empty)),
    ("WordCountSetup", (context => WordCountSetup.run(context), Map.empty)),
    // Main jobs
    ("AnotherSetup", (context => WordCountSetup.run(context), Map.empty)) // FIXME
  )


  def main(args: Array[String]) {
    val defaultSparkConf = Map(
      "spark.logConf" -> "true",
      "spark.executor.extraJavaOptions" -> "-Djava.io.tmpdir=/mnt -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps -XX:-UseGCOverheadLimit",
      "spark.akka.frameSize" -> "500",
      "spark.shuffle.memoryFraction" -> "0.2",
      "spark.storage.memoryFraction" -> "0.3",
      "spark.driver.userClassPathFirst" -> "true",
      "spark.executor.userClassPathFirst" -> "true",
      "spark.hadoop.validateOutputSpecs" -> "true",
      "spark.eventLog.enabled" -> "false" // may break the master with big jobs if true
    )
    CoreJobRunner.runJobSetup(args, availableJobsSetups, defaultSparkConf)
  }
}
