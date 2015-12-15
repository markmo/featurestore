name := "featurestore"

version := "0.1"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/"
//  "RoundEights" at "http://maven.spikemark.net/roundeights"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-sql" % "1.5.2",
  "com.github.mdr" %% "ascii-graphs" % "0.0.3",
  "com.databricks" %% "spark-csv" % "1.3.0",
  "com.github.tototoshi" %% "scala-csv" % "1.2.2",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
//  "com.roundeights" %% "hasher" % "1.2.0"
)
