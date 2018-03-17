import AssemblyKeys._  

assemblySettings

net.virtualvoid.sbt.graph.Plugin.graphSettings

name := "StackOverflow"

version := "0.1"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
 cp filter {x => x.data.getName.matches(".*incubating.*") || x.data.getName.matches(".*test.*") ||  x.data.getName.matches(".*minlog.*")  || x.data.getName.matches(".*metrics.*")}
}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case x => old(x)
  }
}
