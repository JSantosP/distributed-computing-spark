organization := "course"

name := "distributed-computing-spark"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.6"

libraryDependencies in ThisBuild <++= scalaVersion { (sv: String) =>
  val sparkVersion = "1.3.0"
  Seq(
		"org.apache.spark" %% "spark-core" % sparkVersion,
		"org.apache.spark" %% "spark-sql" % sparkVersion,
		"io.spray" %%  "spray-json" % "1.3.1",
    "org.slf4j" % "slf4j-simple" % "1.7.5")
}
