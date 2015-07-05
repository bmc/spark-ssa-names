name := """ssn-names"""

version := "1.0"

//scalaVersion := "2.11.6"
// Spark is currently compiled against 2.10. Don't compile against 2.11.
scalaVersion := "2.10.5"

scalacOptions := Seq("-deprecation", "-feature")

resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"     % "1.4.0" % "provided",
  "org.apache.spark"  %% "spark-sql"      % "1.4.0"
)

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
