
name := "spark-decision-tree-learning"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.11"

mainClass in Compile := Some("DecisionTreeLearningSQL")

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.example",
  scalaVersion := "2.11.11",
  test in assembly := {}
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("DecisionTreeLearningSQL")
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _ *) => MergeStrategy.discard

  case x =>
    MergeStrategy.first
}

/* including scala bloats your assembly jar unnecessarily, and may interfere with
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "niklausDecisionLearningFullT9Simple.jar"

/* you need to be able to undo the "provided" annotation on the deps when running your spark
   programs locally i.e. from sbt; this bit reincludes the full classpaths in the compile and run tasks. */
fullClasspath in Runtime := (fullClasspath in (Compile, run)).value
