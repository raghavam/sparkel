val owlAPI = "net.sourceforge.owlapi" % "owlapi-distribution" % "4.1.3"
val spark = "org.apache.spark" %% "spark-core" % "1.6.0"

lazy val root = (project in file(".")).
  settings(
    name := "sparkel",
    version := "0.1.0",
    scalaVersion := "2.10.6",
    libraryDependencies += owlAPI,
    libraryDependencies += spark
  )