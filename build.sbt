name := "featurestore"

version := "0.1"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  DefaultMavenRepository,
  "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
//  "RoundEights" at "http://maven.spikemark.net/roundeights"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2" % "provided"
    exclude("org.mortbay.jetty", "servlet-api")
    exclude("commons-beanutils", "commons-beanutils-core")
    exclude("commons-collections", "commons-collections")
    exclude("commons-logging", "commons-logging")
    exclude("com.esotericsoftware.minlog", "minlog"),
  "org.apache.spark" %% "spark-sql" % "1.5.2" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.5.2" % "provided",
  "com.github.mdr" % "ascii-graphs_2.10" % "0.0.3",
  "com.databricks" %% "spark-csv" % "1.3.0"
    exclude("org.apache.spark", "spark-core_2.10")
    exclude("org.apache.spark", "spark-sql_2.10")
  ,
  "com.github.tototoshi" % "scala-csv_2.10" % "1.2.2",
  "com.github.nscala-time" %% "nscala-time" % "2.6.0",
  "net.openhft" % "zero-allocation-hashing" % "0.5",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "ai.h2o" % "sparkling-water-core_2.10" % "1.5.2"
    exclude("org.apache.spark", "spark-core_2.10")
    exclude("org.apache.spark", "spark-sql_2.10")
    exclude("org.scala-lang", "scala-library")
//  "com.roundeights" %% "hasher" % "1.2.0"
)

scalacOptions ++= List(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  //"-Ywarn-value-discard", // fails with @sp on Unit
  "-Xfuture"
)

crossScalaVersions := List(scalaVersion.value)

parallelExecution in test := false

testOptions in Test += Tests.Argument("-oF")

fork in Test := true