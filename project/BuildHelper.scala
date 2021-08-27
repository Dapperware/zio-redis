import sbt._
import Keys._
import sbtbuildinfo._
import BuildInfoKeys._
import scalafix.sbt.ScalafixPlugin.autoImport._

object BuildHelper {
  private val versions: Map[String, String] = {
    import org.snakeyaml.engine.v2.api.{ Load, LoadSettings }

    import java.util.{ List => JList, Map => JMap }
    import scala.jdk.CollectionConverters._

    val doc = new Load(LoadSettings.builder().build())
      .loadFromReader(scala.io.Source.fromFile(".github/workflows/ci.yml").bufferedReader())

    val yaml = doc.asInstanceOf[JMap[String, JMap[String, JMap[String, JMap[String, JMap[String, JList[String]]]]]]]

    val list = yaml.get("jobs").get("test").get("strategy").get("matrix").get("scala").asScala

    list.map(v => (v.split('.').take(2).mkString("."), v)).toMap
  }

  val Scala212: String = versions("2.12")
  val Scala213: String = versions("2.13")
  val Zio: String      = "1.0.11"

  def buildInfoSettings(packageName: String) =
    List(
      buildInfoKeys := List[BuildInfoKey](name, version, scalaVersion, sbtVersion, isSnapshot),
      buildInfoPackage := packageName,
      buildInfoObject := "BuildInfo"
    )

  def stdSettings(prjName: String) =
    List(
      name := s"$prjName",
      crossScalaVersions := List(Scala212, Scala213),
      ThisBuild / scalaVersion := Scala213,
      scalacOptions := stdOptions ++ extraOptions(scalaVersion.value, optimize = !isSnapshot.value),
      ThisBuild / semanticdbEnabled := true,
      ThisBuild / semanticdbOptions += "-P:semanticdb:synthetics:on",
      ThisBuild / semanticdbVersion := scalafixSemanticdb.revision,
      ThisBuild / scalafixScalaBinaryVersion := CrossVersion.binaryScalaVersion(scalaVersion.value),
      ThisBuild / scalafixDependencies ++= List(
        "com.github.liancheng" %% "organize-imports" % "0.5.0",
        "com.github.vovapolu"  %% "scaluzzi"         % "0.1.20"
      ),
      Test / parallelExecution := true,
      incOptions ~= (_.withLogRecompileOnMacro(false)),
      autoAPIMappings := true
    )

  private def extraOptions(scalaVersion: String, optimize: Boolean) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 13)) =>
        List("-Ywarn-unused:params,-implicits") ++ std2xOptions ++ optimizerOptions(optimize)
      case Some((2, 12)) =>
        List(
          "-opt-warnings",
          "-Ywarn-extra-implicit",
          "-Ywarn-unused:_,imports",
          "-Ywarn-unused:imports",
          "-Ypartial-unification",
          "-Yno-adapted-args",
          "-Ywarn-inaccessible",
          "-Ywarn-infer-any",
          "-Ywarn-nullary-override",
          "-Ywarn-nullary-unit",
          "-Ywarn-unused:params,-implicits",
          "-Xfuture",
          "-Xsource:2.13",
          "-Xmax-classfile-name",
          "242"
        ) ++ std2xOptions ++ optimizerOptions(optimize)
      case _ => Nil
    }

  private def optimizerOptions(optimize: Boolean): List[String] =
    if (optimize) List("-opt:l:inline", "-opt-inline-from:zio.internal.**") else Nil

  private val stdOptions =
    List("-deprecation", "-encoding", "UTF-8", "-feature", "-unchecked", "-Xfatal-warnings")

  private val std2xOptions =
    List(
      "-language:higherKinds",
      "-language:existentials",
      "-explaintypes",
      "-Yrangepos",
      "-Xlint:_,-missing-interpolator,-type-parameter-shadow",
      "-Ywarn-numeric-widen",
      "-Ywarn-value-discard"
    )
}
