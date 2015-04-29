#!/bin/bash

set +e
set -x

SPARK_DIR="target/spark-dist"

function downloadSpark {
    mirror="$(curl -s 'https://www.apache.org/dyn/closer.cgi' | grep -o '<strong>[^<]*</strong>' | sed 's/<[^>]*>//g' | head -1)"

    sparkVersion="$1"
    scalaVersion="$2"
    scalaVersionSuffix=""
    if [ "$scalaVersion" = "2.11" ]; then
        scalaVersionSuffix="-scala2.11"
    fi
    targetFile="target/spark-$sparkVersion-$scalaVersion.tgz"
    if [ -f "$targetFile" ]; then
        echo "Spark $sparkVersion for Scala $scalaVersion already downloaded"
        return 0
    else
        echo "Downloading Spark $sparkVersion for Scala $scalaVersion"
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
        targetFile="target/spark-$sparkVersion-$scalaVersion.tgz"
        tar -xvf "$targetFile" --strip-components=1 -C "$SPARK_DIR"
        return 0
    else
        echo "Failed to download Spark"
        return 1
    fi
}

function toAbsolutePath {
    path="$1"
    echo "$(perl -e "use File::Spec; print File::Spec->rel2abs(\"$path\")")"
}

function remove_duplicates {
    result=$(echo "$1" | awk -v RS=':' -v ORS=":" '!a[$1]++{if (NR > 1) printf ORS; printf $a[$1]}')
    echo $result
}

function shutdown {
    $SPARK_HOME/sbin/spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 2
    $SPARK_HOME/sbin/stop-master.sh
}

echo "Compiling everything and packaging against Scala 2.10"
sbt/sbt clean package it:package
if [ "$?" != "0" ]; then
    echo "Failed to build artifacts"
    exit 1
fi

installSpark "1.2.1" "2.10"
if [ "$?" != "0" ]; then
    echo "Failed to install Spark"
    exit 1
fi

echo "Preparing classpath"
classPathEntries=""
for pathEntry in $(find . -name "*.jar" | grep -s --colour=never "/target/")
do
    classPathEntries+=":$(toAbsolutePath "$pathEntry")"
done

testClassPathEntries=""
for pathEntry in $(sbt/sbt pureTestClasspath | grep "TEST_CLASSPATH=" | sed -n 's/^TEST_CLASSPATH\=\(.*\)$/\1/p')
do
    testClassPathEntries+=":$pathEntry"
done

export SPARK_SUBMIT_CLASSPATH="$(remove_duplicates "$classPathEntries$testClassPathEntries")"

echo "Running Spark cluster"
export SPARK_HOME="$(toAbsolutePath "$SPARK_DIR")"

export SPARK_MASTER_IP="127.0.0.1"
export SPARK_MASTER_PORT="7777"
export SPARK_MASTER_WEBUI_PORT="8777"
export SPARK_LOG_DIR="$(toAbsolutePath 'target/log')"

trap shutdown SIGINT

$SPARK_HOME/sbin/start-master.sh

export SPARK_WORKER_PORT="7666"
export SPARK_WORKER_WEBUI="8666"

export IT_TEST_SPARK_MASTER="spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"
$SPARK_HOME/sbin/spark-daemon.sh start org.apache.spark.deploy.worker.Worker 2 "$IT_TEST_SPARK_MASTER"

echo "Running tests for Scala 2.10"
sbt/sbt it:test
if [ "$?" = "0" ]; then
  echo "Tests for Scala 2.10 succeeded"
else
  echo "Tests for Scala 2.10 failed"
  retval=1
fi

shutdown

exit $retval
