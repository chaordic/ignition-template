package ignition.jobs.setups

import java.security.MessageDigest

import ignition.core.jobs.CoreJobRunner.RunnerContext

object UsersPasswordsSetup {

  val usersList: Seq[String] =
    """allan
      |aws
      |chaordic
    """.stripMargin.split("\n")

  val passwordsList: Seq[String] =
    """123456
      |1234567
      |12345678
      |abracadabra
    """.stripMargin.split("\n")

  val md5sList: Seq[String] =
    """b05cc4c5d9c2da10c463ba4edf48d4c9
      |770f98821764790ce6dae1f1b9ca84e3
      |ddead3c9d25d6f31d0ff9eb97ab293c4
      |f637f075da94cc346b2b49bae68ed306
    """.stripMargin.split("\n")

   def run(runnerContext: RunnerContext) {

     val sc = runnerContext.sparkContext
     val sparkConfig = runnerContext.config

     val usersRDD = sc.parallelize(usersList)

     val passwordsRDD = sc.parallelize(passwordsList)
     val md5sRDD = sc.parallelize(md5sList)

     usersRDD.cartesian(passwordsRDD).cartesian(md5sRDD)
       .filter { case ((user, password), md5) =>
         val pair = s"${user}:${password}"
         val currentMD5 =
           MessageDigest.getInstance("MD5").digest(pair.getBytes).map("%02x".format(_)).mkString
         md5 == currentMD5
       }
       .collect()
       .foreach(println)

   }

 }
