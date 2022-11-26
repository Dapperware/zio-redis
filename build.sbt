import BuildHelper._

Global / onChangedBuildSource := ReloadOnSourceChanges

inThisBuild(
  List(
    developers := List(
      Developer("jdegoes", "John De Goes", "john@degoes.net", url("https://degoes.net")),
      Developer("mijicd", "Dejan Mijic", "dmijic@acm.org", url("https://github.com/mijicd"))
    ),
    homepage         := Some(url("https://zio.dev/zio-redis/")),
    licenses         := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    organization     := "dev.zio",
    organizationName := "John A. De Goes and the ZIO contributors",
    startYear        := Some(2021)
  )
)

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

addCommandAlias("compileBenchmarks", "benchmarks/Jmh/compile")
addCommandAlias("compileSources", "example/Test/compile; redis/Test/compile")
addCommandAlias("check", "fixCheck; fmtCheck")
addCommandAlias("fix", "scalafixAll")
addCommandAlias("fixCheck", "scalafixAll --check")
addCommandAlias("fmt", "all scalafmtSbt scalafmtAll")
addCommandAlias("fmtCheck", "all scalafmtSbtCheck scalafmtCheckAll")
addCommandAlias("prepare", "fix; fmt")

lazy val root =
  project
    .in(file("."))
    .settings(publish / skip := true)
    .aggregate(redis, benchmarks, example)
    .enablePlugins(WebsitePlugin)

lazy val redis =
  project
    .in(file("redis"))
    .enablePlugins(BuildInfoPlugin)
    .settings(buildInfoSettings("zio.redis"))
    .settings(scala3Settings)
    .settings(stdSettings("zio-redis"))
    .settings(
      libraryDependencies ++= List(
        "dev.zio"                %% "zio-streams"             % "2.0.4",
        "dev.zio"                %% "zio-logging"             % "2.1.4",
        "dev.zio"                %% "zio-schema"              % "0.3.0",
        "dev.zio"                %% "zio-schema-protobuf"     % "0.3.0" % Test,
        "dev.zio"                %% "zio-test"                % "2.0.4" % Test,
        "dev.zio"                %% "zio-test-sbt"            % "2.0.4" % Test,
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1"
      ),
      testFrameworks := List(new TestFramework("zio.test.sbt.ZTestFramework"))
    )

lazy val benchmarks =
  project
    .in(file("benchmarks"))
    .enablePlugins(JmhPlugin)
    .dependsOn(redis)
    .settings(stdSettings("benchmarks"))
    .settings(
      crossScalaVersions -= Scala3,
      publish / skip := true,
      libraryDependencies ++= List(
        "dev.profunktor"    %% "redis4cats-effects"  % "1.2.0",
        "io.chrisdavenport" %% "rediculous"          % "0.4.0",
        "io.laserdisc"      %% "laserdisc-fs2"       % "0.5.0",
        "dev.zio"           %% "zio-schema-protobuf" % "0.3.1"
      )
    )

lazy val example =
  project
    .in(file("example"))
    .dependsOn(redis)
    .settings(stdSettings("example"))
    .settings(
      publish / skip := true,
      libraryDependencies ++= List(
        "com.softwaremill.sttp.client3" %% "async-http-client-backend-zio" % "3.8.3",
        "com.softwaremill.sttp.client3" %% "zio-json"                      % "3.8.3",
        "dev.zio"                       %% "zio-streams"                   % "2.0.3",
        "dev.zio"                       %% "zio-config-magnolia"           % "3.0.2",
        "dev.zio"                       %% "zio-config-typesafe"           % "3.0.2",
        "dev.zio"                       %% "zio-schema-protobuf"           % "0.3.1",
        "dev.zio"                       %% "zio-json"                      % "0.3.0",
        "io.d11"                        %% "zhttp"                         % "2.0.0-RC11"
      )
    )
