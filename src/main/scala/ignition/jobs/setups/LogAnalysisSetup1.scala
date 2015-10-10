package ignition.jobs.setups

import ignition.core.jobs.CoreJobRunner.RunnerContext

object LogAnalysisSetup1 {

  val sample =
    """
      |0 /pagina.html 200
      |1 /pagina.html 200
      |2 /pagina.html 500
      |3 /pagina.html 500
      |4 /pagina.html 500
      |5 /pagina.html 500
      |6 /pagina.html 401
      |7 /pagina.html 403
      |8 /pagina.html 403
      |9 /pagina.html 201
    """.stripMargin.split("\n").filter(_.trim.nonEmpty)

  def run(runnerContext: RunnerContext) {

    val sc = runnerContext.sparkContext
    val sparkConfig = runnerContext.config
    val rdd = sc.parallelize(sample)
    val status = rdd map { x =>
      x.split(" ")(2)
    }

    val count200 = status filter { x =>
      x == "200"
    } count()

    val count500 = status filter { x =>
      x == "500"
    } count()

    val count401 = status filter { x =>
      x == "401"
    } count()

    val total = rdd.count()

    println(s"Count 200 = ${count200.toFloat / total}")
    println(s"Count 500 = ${count500.toFloat / total}")
    println(s"Count 401 = ${count401.toFloat / total}")

  }

}
