package ignition.jobs.setups

import ignition.core.jobs.CoreJobRunner.RunnerContext

object LogAnalysisSetup2 {

  val addressLines: Seq[String] =
    """
      |1 10.0.0.1
      |2 10.0.0.2
      |3 10.0.0.1
      |4 10.0.0.3
      |5 10.0.0.1
      |6 10.0.0.1
      |7 10.0.0.4
      |8 10.0.0.5
      |9 10.0.0.5
      |10 10.0.0.6
    """.stripMargin.split("\n").filter(_.trim.nonEmpty)
  
  val statusLines: Seq[String] =
     """
       |1 /pagina.html 401
       |2 /pagina.html 401
       |3 /pagina.html 401
       |4 /pagina.html 401
       |5 /pagina.html 401
       |6 /pagina.html 200
       |7 /pagina.html 200
       |8 /pagina.html 200
       |9 /pagina.html 404
       |10 /pagina.html 500
     """.stripMargin.split("\n").filter(_.trim.nonEmpty)

   def run(runnerContext: RunnerContext) {

     val sc = runnerContext.sparkContext
     val sparkConfig = runnerContext.config

     val statusRDD = sc.parallelize(statusLines)
     val status = statusRDD map { x =>
       val tks = x.split(" ")
       (tks(0), tks(2))
     }

     val ipsRDD = sc.parallelize(addressLines)
     val ips = ipsRDD map { x =>
       val tks = x.split(" ")
       (tks(0), tks(1))
     }

     val distinctAffectedIps = status join(ips) map { case (_, t) =>
       t
     } filter { case (_status, _ip) =>
       _status == "401"
     } map { case (_status, _ip) =>
       _ip
     } distinct() count()

     val distinctTotalIps = ips map { case (_, _ip) =>
       _ip
     } distinct() count()

     println(s"Proportion of affected IPs = ${distinctAffectedIps.toFloat / distinctTotalIps}")


   }

 }
