ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18" // REQUIRED for Spark 3.5.1

lazy val root = (project in file("."))
  .settings(
    name := "SparkDataFrameExample"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql"  % "3.5.1"
)

Compile / run / fork := true
Compile / run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
