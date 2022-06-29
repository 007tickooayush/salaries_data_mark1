ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "salaries_data_mark1",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.1",
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.2.1"
  )
