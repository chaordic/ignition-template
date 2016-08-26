import sbt._
import sbt.Keys._

object IgnitionBuild extends Build {


  lazy val root = project.in(file("."))
    .dependsOn(file("core"))
    .aggregate(file("core"))
    .settings(
      name := "ignition-template",
      version := "1.0",
      scalaVersion := "2.11.8",
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings", "-Xmax-classfile-name", "130"),
      // Because we can't run two spark contexts on same VM
      parallelExecution in Test := false,
      resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      libraryDependencies += ("org.apache.spark" %% "spark-core" % "2.0.0" % "provided")
        .exclude("org.apache.hadoop", "hadoop-client"),

      libraryDependencies += ("org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"),
      libraryDependencies += ("org.apache.hadoop" % "hadoop-client" % "2.7.2" % "provided"),
      libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.9",
      libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.13",
      libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
  )
}
