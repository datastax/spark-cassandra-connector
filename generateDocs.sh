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
    sbt doc
    mkdir $OUTPUT/$VERSION
    
    for FOLDER in $SCC_HOME/spark*; do
        echo "COPYING $FOLDER to $OUTPUT/$VERSION/$MODULE"
        MODULE=$(basename $folder)
        cp -vr $FOLDER/target/scala-2.10/api $OUTPUT/$VERSION/$MODULE
    done
done
git checkout gh-pages
cp -r $OUTPUT/* $SCC_HOME/ApiDocs
