ThisBuild / organization     := "io.github.pityka"
ThisBuild / scalaVersion     := "3.3.3"
ThisBuild / versionScheme    := Some("early-semver")
ThisBuild / dynverSeparator  := "-"

lazy val ghOwner = settingKey[String]("GitHub owner of the repo for Packages publishing.")
lazy val ghRepo  = settingKey[String]("GitHub repo name for Packages publishing.")

lazy val root = (project in file("."))
  .settings(
    name    := "fs2csv",
    ghOwner := sys.env.getOrElse("GH_OWNER", "pityka"),
    ghRepo  := sys.env.getOrElse("GH_REPO", "fs2csv"),

    libraryDependencies ++= Seq(
      "io.github.pityka" %% "saddle-io"            % "4.0.0-M14",
      "io.github.pityka" %% "saddle-core"          % "4.0.0-M14",
      "co.fs2"           %% "fs2-core"             % "3.10.2",
      "co.fs2"           %% "fs2-io"               % "3.10.2",
      "org.gnieh"        %% "fs2-data-csv"         % "1.14.1" % Test,
      "org.gnieh"        %% "fs2-data-csv-generic" % "1.14.1" % Test,
      "org.scalameta"    %% "munit"                % "1.0.0"  % Test,
      "org.typelevel"    %% "munit-cats-effect"    % "2.0.0"  % Test
    ),

    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Wunused:imports",
      "-Wunused:privates"
    ),

    Compile / doc / sources                := Seq.empty,
    Compile / packageDoc / publishArtifact := false,

    publishMavenStyle      := true,
    Test / publishArtifact := false,
    publishTo              := Some(
      "GitHub Packages" at s"https://maven.pkg.github.com/${ghOwner.value}/${ghRepo.value}"
    ),
    credentials += Credentials(
      "GitHub Package Registry",
      "maven.pkg.github.com",
      sys.env.getOrElse("GITHUB_ACTOR", ""),
      sys.env.getOrElse("GITHUB_TOKEN", "")
    ),

    Test / fork := true,
    run / fork  := true,
    run / javaOptions ++= Seq("-Xms2g", "-Xmx4g")
  )
