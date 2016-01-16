packAutoSettings
val spark = "org.apache.spark" %% "spark-core" % "1.6.0"
val owlAPI = "net.sourceforge.owlapi" % "owlapi-distribution" % "4.1.3"
val elk = "org.semanticweb.elk" % "elk-owlapi" % "0.4.3"

lazy val root = (project in file(".")).
  settings(
    name := "sparkel",
    version := "0.1.0",
    scalaVersion := "2.10.6",
    libraryDependencies += spark,
    libraryDependencies += owlAPI,
    libraryDependencies += elk
  )  