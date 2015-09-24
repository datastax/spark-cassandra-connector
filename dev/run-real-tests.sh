#!/bin/bash

set +e

FWDIR="$(cd "$(dirname "$0")"/..; pwd)"
if [ "$(pwd)" != "$FWDIR" ]; then
    echo "Please run this file from the root project directory."
    exit 1
fi

if [ "$#" -ne "2" ]; then
    echo "Usage: dev/run-real-tests.sh <Spark version> <Scala version>"
    echo "where"
    echo "  <Spark version> is one of the versions available on Apache mirror"
    echo "  <Scala version> is either 2.10 or 2.11"
    exit 1
fi

SPARK_DIR="target/spark-dist"
SPARK_ARCHIVES_DIR="target/spark-archives"

TARGET_SPARK_VERSION="$1"
TARGET_SCALA_VERSION="$2"

scala211=""
if [ "$TARGET_SCALA_VERSION" = "2.11" ]; then
    scala211="-Dscala-2.11=true"
fi

function downloadSpark {
    mirror="$(curl -s 'https://www.apache.org/dyn/closer.cgi' | grep -o '<strong>[^<]*</strong>' | sed 's/<[^>]*>//g' | head -1)"

    sparkVersion="$1"
    scalaVersion="$2"
    scalaVersionSuffix=""
    if [ "$scalaVersion" = "2.11" ]; then
        scalaVersionSuffix="-scala2.11"
    fi
    targetFile="$SPARK_ARCHIVES_DIR/spark-$sparkVersion-$scalaVersion.tgz"
    if [ -f "$targetFile" ]; then
        echo "Spark $sparkVersion for Scala $scalaVersion already downloaded"
        return 0
    else
        echo "Downloading Spark $sparkVersion for Scala $scalaVersion"
        mkdir -p "$SPARK_ARCHIVES_DIR"
        wget -O "$targetFile" "$mirror""spark/spark-$sparkVersion/spark-$sparkVersion-bin-hadoop1$scalaVersionSuffix.tgz"
        return "$?"
    fi
}

function installSpark {
    sparkVersion="$1"
    scalaVersion="$2"
    downloadSpark "$sparkVersion" "$scalaVersion"
    if [ "$?" = "0" ]; then
        echo "Installing Spark $sparkVersion for Scala $scalaVersion"
        rm -rf "$SPARK_DIR"
        mkdir -p "$SPARK_DIR"
        targetFile="$SPARK_ARCHIVES_DIR/spark-$sparkVersion-$scalaVersion.tgz"
        tar -xvf "$targetFile" --strip-components=1 -C "$SPARK_DIR"
        return 0
    else
        echo "Failed to download Spark"
        return 1
    fi
}

function toAbsolutePath {
    path="$1"
    perl -e "use File::Spec; print File::Spec->rel2abs(\"$path\")"
}

function remove_duplicates {
    result=$(echo "$1" | awk -v RS=':' -v ORS=":" '!a[$1]++{if (NR > 1) printf ORS; printf $a[$1]}')
    echo "$result"
}

function shutdown {
    "$SPARK_HOME"/sbin/spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 2
    "$SPARK_HOME"/sbin/stop-master.sh
    rm -rf "$SPARK_DIR"/work
}

function prepareClasspath {
    echo "Preparing classpath"
    classPathEntries=""
    for pathEntry in $(find . -name "spark-cassandra-connector*.jar" | grep -s --colour=never "/target/scala-$TARGET_SCALA_VERSION" | grep -s --colour=never -v "/cassandra-server/" | grep -s --colour=never -v "$SPARK_DIR")
    do
        classPathEntries+=":$(toAbsolutePath "$pathEntry")"
    done

    testClassPathEntries=""
    for pathEntry in $(sbt/sbt "$scala211" pureTestClasspath | grep -s --colour=never "TEST_CLASSPATH=" | sed -n 's/^TEST_CLASSPATH\=\(.*\)$/\1/p')
    do
        testClassPathEntries+=":$pathEntry"
    done

    remove_duplicates "$classPathEntries$testClassPathEntries"
}


echo "Compiling everything and packaging against Scala $TARGET_SCALA_VERSION"
sbt/sbt "$scala211" clean test:package it:package assembly
if [ "$?" != "0" ]; then
    echo "Failed to build artifacts"
    exit 1
fi

export SPARK_SUBMIT_CLASSPATH="$(prepareClasspath)"

installSpark "$TARGET_SPARK_VERSION" "$TARGET_SCALA_VERSION"
if [ "$?" != "0" ]; then
    echo "Failed to install Spark"
    exit 1
fi

echo "Running Spark cluster"
export SPARK_HOME="$(toAbsolutePath "$SPARK_DIR")"

export SPARK_MASTER_IP="127.0.0.1"
export SPARK_MASTER_PORT="7777"
export SPARK_MASTER_WEBUI_PORT="8777"
export SPARK_LOG_DIR="$(toAbsolutePath 'target/log')"

trap shutdown SIGINT

"$SPARK_HOME"/sbin/start-master.sh

export SPARK_WORKER_PORT="7666"
export SPARK_WORKER_WEBUI="8666"

export IT_TEST_SPARK_MASTER="spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"
"$SPARK_HOME"/sbin/spark-daemon.sh start org.apache.spark.deploy.worker.Worker 2 "$IT_TEST_SPARK_MASTER"

echo "Running tests for Spark $TARGET_SPARK_VERSION and Scala $TARGET_SCALA_VERSION"
sbt/sbt "$scala211" it:test
if [ "$?" = "0" ]; then
  echo "Tests succeeded"
else
  echo "Tests failed"
  retval=1
fi

shutdown

exit $retval
