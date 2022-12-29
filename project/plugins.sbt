addSbtPlugin("ch.epfl.scala"                     % "sbt-bloop"        % "1.5.4")
addSbtPlugin("ch.epfl.scala"                     % "sbt-scalafix"     % "0.10.4")
addSbtPlugin("com.eed3si9n"                      % "sbt-buildinfo"    % "0.11.0")
addSbtPlugin("com.github.sbt"                    % "sbt-ci-release"   % "1.5.11")
addSbtPlugin("com.github.sbt"                    % "sbt-unidoc"       % "0.5.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")
addSbtPlugin("de.heikoseeberger"                 % "sbt-header"       % "5.9.0")
addSbtPlugin("org.scalameta"                     % "sbt-mdoc"         % "2.3.6")
addSbtPlugin("org.scalameta"                     % "sbt-scalafmt"     % "2.5.0")
addSbtPlugin("pl.project13.scala"                % "sbt-jmh"          % "0.4.3")
addSbtPlugin("dev.zio"                           % "zio-sbt-website"  % "0.1.5+27-a79a4f13-SNAPSHOT")

libraryDependencies += "org.snakeyaml" % "snakeyaml-engine" % "2.5"

resolvers ++= Resolver.sonatypeOssRepos("public")
