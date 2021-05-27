name := "capstone-spark-project"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "com.github.pureconfig" %% "pureconfig" % "0.15.0"
)
