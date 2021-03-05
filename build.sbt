name := "spark-optics"

scalaVersion := "2.12.9"

crossScalaVersions := Seq("2.11.12", "2.12.13")

parallelExecution in Test := false
fork in Test := true
javaOptions ++= Seq("-Xms512M",
  "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled")

lazy val sparkVersion = settingKey[String]("the spark version")
sparkVersion := (scalaBinaryVersion.value match {
  case "2.11" => "2.4.5"
  case "2.12" => "3.0.1"
})

lazy val sparkTestVersion = settingKey[String]("the spark test")
sparkTestVersion := (scalaBinaryVersion.value match {
  case "2.11" => "0.14.0"
  case "2.12" => "1.0.0"
})

libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion.value}_${sparkTestVersion.value}" % "test"
  )
}

licenses := Seq(
  "Apache License 2.0" -> url(
    "http://www.apache.org/licenses/LICENSE-2.0.html"))

inThisBuild(List(
  organization := "org.hablapps",
  homepage := Some(url("https://github.com/hablapps/sparkOptics")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "Alfonso",
      "Alfonso Roa Redondo",
      "alfonso.roa@hablapps.com",
      url("https://github.com/alfonsorr")
    )
  )
))
