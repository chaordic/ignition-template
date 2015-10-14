package ignition.jobs.setups

import ignition.core.jobs.CoreJobRunner.RunnerContext

object LogAnalysisSetup3 {
  
  val statusLines: Seq[String] =
     """1 /comprar 200
       |2 /comprar 200
       |3 /comprar 200
       |4 /listar 200
       |5 /listar 200
       |6 /ver 200
       |7 /ver 200
       |8 /excluir 200
       |9 /excluir 200
       |10 /teste 501
       |11 /teste 200
       |12 /listrr 404""".stripMargin.split("\n")

   def run(runnerContext: RunnerContext) {

     val sc = runnerContext.sparkContext
     val sparkConfig = runnerContext.config

     val statusRDD = sc.parallelize(statusLines)

     val pages = statusRDD.map { x =>
       x.split(" ")(1)
     }

     val total = statusRDD.count()

     pages
       .map { p => (p, 1) }
       .reduceByKey { _ + _ }
       .sortBy({ case (page, count) => count }, false)
       .take(5)
       .foreach { case (page, count) =>
         println(s"${page} ${count} ${count.toFloat / total}")
       }


   }

 }
