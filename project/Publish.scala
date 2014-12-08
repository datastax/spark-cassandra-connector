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

  lazy val creds = (for {
    publish <- Option(Path.userHome / ".ivy2" / ".credentials")
    if publish.exists
  } yield Seq(credentials += Credentials(publish))).getOrElse(Seq.empty)

  override lazy val settings = creds ++ Seq(
    publishTo <<= version { v: String =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
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