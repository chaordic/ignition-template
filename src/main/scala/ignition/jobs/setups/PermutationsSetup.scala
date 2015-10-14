package ignition.jobs.setups

import ignition.core.jobs.CoreJobRunner.RunnerContext

object PermutationsSetup {

  val usersList: Seq[String] =
    """roseboro
      |bororose
      |howlet
      |telwoh
      |aless
      |sales
      |xyz""".stripMargin.split("\n")

   def run(runnerContext: RunnerContext) {

     val sc = runnerContext.sparkContext
     val sparkConfig = runnerContext.config

     val usersRDD = sc.parallelize(usersList)

     val words =
       usersRDD
         .flatMap { x => x.split(" ")}
         .keyBy { x => x.sorted }
         .groupByKey()
         .map { case (_, it) => it.toSet }
         .filter { x => x.size > 1 }
         .map { x => x.mkString(" ") }

     words.collect() foreach println

   }

 }
