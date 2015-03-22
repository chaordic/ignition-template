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
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings"),
      // Because we can't run two spark contexts on same VM
      parallelExecution in Test := false,
      libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.2.1" % "provided")
        .exclude("org.apache.hadoop", "hadoop-client"),
      libraryDependencies += ("org.apache.hadoop" % "hadoop-client" % "1.0.4" % "provided"),
      libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.6",
      libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
  )
}
