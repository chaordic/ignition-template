import sbt._
import sbt.Keys._

object IgnitionBuild extends Build {


  lazy val root = project.in(file("."))
    .dependsOn(file("core"))
    .aggregate(file("core"))
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(
      name := "ignition-template",
      version := "1.0",
      scalaVersion := "2.10.4",
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings", "-Xmax-classfile-name", "130"),
      // Because we can't run two spark contexts on same VM
      parallelExecution in Test := false,
      resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.1" % "provided")
        .exclude("org.apache.hadoop", "hadoop-client"),

      libraryDependencies += ("org.apache.spark" %% "spark-sql" % "1.5.1" % "provided"),
      libraryDependencies += ("org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.7.1" % "provided"),
      libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.6",
      libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.13",
      libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"
  )
}
