name := "featurestore"

version := "0.1"

scalaVersion := "2.10.6"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  DefaultMavenRepository
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2" % "provided" excludeAll (
    ExclusionRule(organization = "org.scala-lang"),
    ExclusionRule("jline", "jline"),
    ExclusionRule("org.slf4j", "slf4j-api"),
    ExclusionRule("org.mortbay.jetty", "servlet-api"),
    ExclusionRule("commons-beanutils", "commons-beanutils-core"),
    ExclusionRule("commons-collections", "commons-collections"),
    ExclusionRule("commons-logging", "commons-logging"),
    ExclusionRule("com.esotericsoftware.minlog", "minlog")
    ),
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
  "com.typesafe" % "config" % "1.3.0",
  "org.json4s" %% "json4s-native" % "3.3.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "ai.h2o" % "sparkling-water-core_2.10" % "1.5.2"
    exclude("org.apache.spark", "spark-core_2.10")
    exclude("org.apache.spark", "spark-sql_2.10")
    exclude("org.scala-lang", "scala-library"),

  // topnotch dependencies
  "io.spray" %% "spray-json" % "1.3.2",
  //"com.typesafe" % "config" % "1.2.1",
  "com.iheart" %% "ficus" % "1.0.2"
  //"joda-time" % "joda-time" % "2.9.1",
  //"org.joda" % "joda-convert" % "1.8"
)

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

publishTo := Some("Artifactory Realm" at "http://localhost:8081/artifactory/libs-release-local")

credentials += Credentials("Artifactory Realm", "localhost", "admin", "password")

scalacOptions ++= List(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xfatal-warnings",
//  "-Xlint",
  "-Yno-adapted-args",
//  "-Ywarn-dead-code",
//  "-Ywarn-numeric-widen",
//  "-Ywarn-value-discard", // fails with @sp on Unit
//  "-Xlog-implicits",
  "-Xfuture"
)

crossScalaVersions := List(scalaVersion.value)

parallelExecution in test := false

testOptions in Test += Tests.Argument("-oF")

fork in Test := true