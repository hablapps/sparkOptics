name := "spark-optics"

version := "0.1"

scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.11.12", "2.12.8")

parallelExecution in Test := false
fork in Test := true
javaOptions ++= Seq("-Xms512M",
  "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled")

libraryDependencies ++= {
  val sparkVersion = "2.3.1"
  Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.10.0" % "test",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "test"
  )
}

licenses := Seq(
  "Apache License 2.0" -> url(
    "http://www.apache.org/licenses/LICENSE-2.0.html"))
