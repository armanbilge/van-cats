ThisBuild / baseVersion := "0.0"

ThisBuild / organization := "com.armanbilge"
ThisBuild / publishGithubUser := "armanbilge"
ThisBuild / publishFullName := "Arman Bilge"
ThisBuild / startYear := Some(2021)

mimaPreviousArtifacts := Set()

enablePlugins(SonatypeCiReleasePlugin)
ThisBuild / homepage := Some(url("https://github.com/armanbilge/van-cats"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/armanbilge/van-cats"),
    "git@github.com:armanbilge/van-cats.git"))
sonatypeCredentialHost := "s01.oss.sonatype.org"

val Scala213 = "2.13.6"
val Scala3 = "3.0.0"
ThisBuild / crossScalaVersions := Seq(Scala3, Scala213)

replaceCommandAlias(
  "ci",
  "; project /; headerCheckAll; scalafmtCheckAll; scalafmtSbtCheck; clean; testIfRelevant; mimaReportBinaryIssuesIfRelevant"
)
addCommandAlias("prePR", "; root/clean; +root/scalafmtAll; scalafmtSbt; +root/headerCreate")

val CatsEffectVersion = "3.1.1"
val Fs2Version = "3.0.4"

lazy val root =
  project.aggregate(core).enablePlugins(NoPublishPlugin)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "van-cats",
    sonatypeCredentialHost := "s01.oss.sonatype.org",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % CatsEffectVersion,
      "co.fs2" %% "fs2-core" % Fs2Version,
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb
        .compiler
        .Version
        .scalapbVersion % "protobuf",
      "org.typelevel" %% "cats-effect-testing-specs2" % "1.1.1" % Test,
      "org.specs2" %% "specs2-core" % "4.12.0" % Test cross CrossVersion.for3Use2_13
    )
  )
  .enablePlugins(Fs2Grpc)
