val ZioSbtVersion = "0.4.0-alpha.30"

addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings"  % "3.0.2")
addSbtPlugin("dev.zio"                           % "zio-sbt-ci"        % ZioSbtVersion)
addSbtPlugin("dev.zio"                           % "zio-sbt-ecosystem" % ZioSbtVersion)
addSbtPlugin("dev.zio"                           % "zio-sbt-website"   % ZioSbtVersion)
