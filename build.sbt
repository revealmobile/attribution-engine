name := "attribution-engine"
version := "0.0.1"
scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.6" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.6" % "provided",
  "org.apache.httpcomponents" % "httpclient" % "4.5.11",
  "org.rogach" %% "scallop" % "3.1.5",
  "com.revealmobile" % "spark-util" % "0.3.2-SNAPSHOT",
  "software.amazon.awssdk" % "s3" % "2.10.66",
    //serdes
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.6.7",
  // security
  "com.amazonaws" % "aws-java-sdk-secretsmanager" % "1.11.714",
  // S3
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.905",
  //di
  "net.codingwell" %% "scala-guice" % "4.2.1",
  // configuration
  "com.typesafe" % "config" % "1.3.3",

  //testing
  "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.scalatestplus" %% "mockito-3-4" % "3.2.2.0" % Test
)

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.1",
  )
}

// packaging
// https://github.com/sbt/sbt-assembly/issues/362
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x => MergeStrategy.first
}
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + module.revision + "." + artifact.extension
}
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

//push assembled JAR to S3
enablePlugins(S3Plugin)
s3Host in s3Upload := "reveal-ci"
mappings in s3Upload := {
  val jarName = s"${name.value}-${version.value}.jar"
  val file = new java.io.File(s"target/scala-2.11/${jarName}")
  val prefix = "deploy/emr"
  Seq((file, s"${prefix}/${jarName}"))
}




