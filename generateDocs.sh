#!/bin/bash
SCC_HOME=$HOME/repos/spark-cassandra-connector
OUTPUT="/tmp/SCC_DOC_TEMP"
rm -r $OUTPUT
mkdir -p $OUTPUT
echo "SPARK CASSANDRA CONNECTOR HOME IS $SCC_HOME"
for VERSION in $@ ;do
    echo "Making docs for $VERSION"
    git checkout "v$VERSION"
    if [ $? -ne 0 ]; then
        echo "Unable to checkout version $VERSION, skipping"
        continue
    fi
    sbt clean
    sbt -Dscala-2.11=true doc
    mkdir $OUTPUT/$VERSION
    
    for MODULE in spark-cassandra-connector spark-cassandra-connector-embedded; do
        FOLDER=$SCC_HOME/$MODULE
        echo "COPYING $FOLDER to $OUTPUT/$VERSION/$MODULE"
        cp -vr $FOLDER/target/scala-2.11/api $OUTPUT/$VERSION/$MODULE
    done
done
git checkout gh-pages
cp -r $OUTPUT/* $SCC_HOME/ApiDocs
