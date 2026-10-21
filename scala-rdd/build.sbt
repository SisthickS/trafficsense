// TrafficSense - Scala RDD demonstration module.
// Build:  sbt -batch package      ->  target/scala-2.12/trafficsense-rdd_2.12-0.1.0.jar
// Test:   sbt -batch test
// Run:    see scripts/run_scala_rdd.sh (spark-submit against the local Spark 3.5.9 install)

ThisBuild / organization := "edu.trafficsense"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"   // must match the Scala version Spark 3.5.x is built with

val sparkVersion = "3.5.9"

lazy val root = (project in file("."))
  .settings(
    name := "trafficsense-rdd",
    libraryDependencies ++= Seq(
      // "provided": spark-submit supplies Spark at runtime, so the jar stays small (only our classes)
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.scalatest"    %% "scalatest"  % "3.2.19"     % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-encoding", "utf8"),
    Test / fork := true,
    Test / javaOptions ++= Seq("-Xmx1g"),
    Compile / mainClass := Some("trafficsense.rdd.TrafficRddDemo")
  )
