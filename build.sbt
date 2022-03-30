ThisBuild / tlBaseVersion := "0.0"

ThisBuild / organization := "com.armanbilge"
ThisBuild / organizationName := "Arman Bilge"
ThisBuild / developers += tlGitHubDev("armanbilge", "Arman Bilge")
ThisBuild / startYear := Some(2021)

ThisBuild / tlUntaggedAreSnapshots := false
ThisBuild / tlSonatypeUseLegacyHost := false

val Scala3 = "3.1.1"
ThisBuild / crossScalaVersions := Seq(Scala3)

val CatsEffectVersion = "3.3.9"
val Fs2Version = "3.2.6"
val Ip4sVersion = "3.1.2"
val ScodecVersion = "2.1.0"
val Specs2Version = "4.12.9"

ThisBuild / scalacOptions ++= Seq("-new-syntax", "-indent", "-source:future")

lazy val root = tlCrossRootProject.aggregate(core)

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
      "org.typelevel" %%% "cats-effect-testing-specs2" % "1.4.0" % Test
    )
  )
  .jsSettings(scalaJSLinkerConfig ~= (_.withModuleKind(ModuleKind.CommonJSModule)))
