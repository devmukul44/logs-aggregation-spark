name := "logs-aggregation"

organization := "com.whiletruecurious"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

// Spark Library Dependencies
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion

// Other Library Dependencies
libraryDependencies += "joda-time" % "joda-time" % "2.9.4"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
