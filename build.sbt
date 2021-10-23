ThisBuild / baseVersion := "0.0"

ThisBuild / organization := "com.armanbilge"
ThisBuild / publishGithubUser := "armanbilge"
ThisBuild / publishFullName := "Arman Bilge"
ThisBuild / startYear := Some(2021)

mimaPreviousArtifacts := Set()

ThisBuild / homepage := Some(url("https://github.com/armanbilge/van-cats"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/armanbilge/van-cats"),
    "git@github.com:armanbilge/van-cats.git"))
sonatypeCredentialHost := "s01.oss.sonatype.org"

val Scala3 = "3.1.0"
ThisBuild / crossScalaVersions := Seq(Scala3)

replaceCommandAlias(
  "ci",
  "; project /; headerCheckAll; scalafmtCheckAll; scalafmtSbtCheck; clean; testIfRelevant; mimaReportBinaryIssuesIfRelevant"
)
replaceCommandAlias(
  "release",
  "; reload; project /; +mimaReportBinaryIssuesIfRelevant; +publishIfRelevant; sonatypeBundleRelease"
)
addCommandAlias("prePR", "; root/clean; +root/scalafmtAll; scalafmtSbt; +root/headerCreate")

val CatsEffectVersion = "3.2.9"
val Fs2Version = "3.1.6"
val Ip4sVersion = "3.1.0"
val ScodecVersion = "2.1.0"
val Specs2Version = "4.12.9"

val commonSettings = Seq(
  scalacOptions ++=
    Seq("-new-syntax", "-indent", "-source:future"),
  sonatypeCredentialHost := "s01.oss.sonatype.org"
)

lazy val root =
  project.aggregate(core.jvm, core.js).enablePlugins(NoPublishPlugin)

lazy val core = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .in(file("core"))
  .settings(
    name := "van-cats",
    libraryDependencies ++= Seq(
      "org.typelevel" %%% "cats-effect" % CatsEffectVersion,
      "co.fs2" %%% "fs2-io" % Fs2Version,
      "com.comcast" %%% "ip4s-core" % Ip4sVersion,
      "org.scodec" %%% "scodec-core" % ScodecVersion,
      "org.typelevel" %%% "cats-effect-testing-specs2" % "1.3.0" % Test
    ),
    scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.CommonJSModule))
  )
  .settings(commonSettings)
