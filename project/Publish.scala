/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import sbt.Keys._
import sbtrelease.ReleasePlugin._

object Publish extends Build {

  val altReleaseDeploymentRepository = sys.props.get("publish.repository.releases")
  val altSnapshotDeploymentRepository = sys.props.get("publish.repository.snapshots")

  val nexus = "https://oss.sonatype.org"
  val defaultReleaseDeploymentRepository = nexus + "/service/local/staging/deploy/maven2"
  val defaultSnapshotDeploymentRepository = nexus + "/content/repositories/snapshots"

  val releasesDeploymentRepository =
    "releases" at (altReleaseDeploymentRepository getOrElse defaultReleaseDeploymentRepository)
  val snapshotsDeploymentRepository =
    "snapshots" at (altSnapshotDeploymentRepository getOrElse defaultSnapshotDeploymentRepository)

  lazy val inlineCredentials = for (
    realm ← sys.props.get("publish.repository.credentials.realm");
    host ← sys.props.get("publish.repository.credentials.host");
    user ← sys.props.get("publish.repository.credentials.user");
    password ← sys.props.get("publish.repository.credentials.password")
  ) yield Credentials(realm, host, user, password)

  lazy val resolvedCredentials = inlineCredentials getOrElse {
    val altCredentialsLocation = sys.props.get("publish.repository.credentials.file").map(new File(_))
    val defaultCredentialsLocation = Path.userHome / ".ivy2" / ".credentials"
    val credentialsLocation = altCredentialsLocation getOrElse defaultCredentialsLocation

    Credentials(credentialsLocation)
  }

  println(s"Using $releasesDeploymentRepository for releases")
  println(s"Using $snapshotsDeploymentRepository for snapshots")

  lazy val creds = Seq(credentials += resolvedCredentials)

  override lazy val settings = creds ++ Seq(
    organizationName := "DataStax",
    organizationHomepage := Some(url("http://www.datastax.com/")),

    publishTo <<= version { v: String =>
      if (v.trim.endsWith("SNAPSHOT"))
        Some(snapshotsDeploymentRepository)
      else
        Some(releasesDeploymentRepository)
    },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    publishArtifact in IntegrationTest := false,
    pomIncludeRepository := { x => false },
    pomExtra :=
      <scm>
        <url>git@github.com:datastax/spark-cassandra-connector.git</url>
        <connection>scm:git:git@github.com:datastax/spark-cassandra-connector.git</connection>
      </scm>
      <developers>
        <developer>
          <id>pkolaczk</id>
          <name>Piotr Kolaczkowski</name>
          <url>http://github.com/pkolaczk</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>jacek-lewandowski</id>
          <name>Jacek Lewandowski</name>
          <url>http://github.com/jacek-lewandowski</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>helena</id>
          <name>Helena Edelson</name>
          <url>http://github.com/helena</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>alexliu68</id>
          <name>Alex Liu</name>
          <url>http://github.com/alexliu68</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>RussellSpitzer</id>
          <name>Russell Spitzer</name>
          <url>http://github.com/RussellSpitzer</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>artem-aliev</id>
          <name>Artem Aliev</name>
          <url>http://github.com/artem-aliev</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
        <developer>
          <id>bcantoni</id>
          <name>Brian Cantoni</name>
          <url>http://github.com/bcantoni</url>
          <organization>DataStax</organization>
          <organizationUrl>http://www.datastax.com/</organizationUrl>
        </developer>
      </developers>
      <contributors>
        <contributor>
          <name>Andrew Ash</name>
          <url>http://github.com/ash211</url>
        </contributor>
        <contributor>
          <name>Luis Angel Vicente Sanchez</name>
          <url>http://github.com/lvicentesanchez</url>
        </contributor>
        <contributor>
          <name>Todd</name>
          <url>http://github.com/tsindot</url>
        </contributor>
        <contributor>
          <name>Li Geng</name>
          <url>http://github.com/anguslee</url>
        </contributor>
        <contributor>
          <name>Isk</name>
          <url>http://github.com/criticaled</url>
        </contributor>
        <contributor>
          <name>Holden Karau</name>
          <url>http://github.com/holdenk</url>
        </contributor>
        <contributor>
          <name>Philipp Hoffmann</name>
          <url>http://github.com/philipphoffmann</url>
        </contributor>
      </contributors>
  )
}