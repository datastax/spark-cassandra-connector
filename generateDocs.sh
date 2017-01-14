#!/bin/bash
SCC_HOME=~/repos/spark-cassandra-connector
OUTPUT=/tmp/SCC_DOC_TEMP
rm -r $OUTPUT
mkdir -p $OUTPUT
for version in $@ ;do
    cd $SCC_HOME
    echo "Making docs for $version"
    
    git checkout "v$version"
    if [ $? -ne 0 ]; then
        echo "Unable to checkout version $version, skipping"
        continue
    fi
    
    cd $SCC_HOME
    sbt clean
    sbt doc
    mkdir $OUTPUT/$version
    
    cd $OUTPUT/$version
    for folder in $SCC_HOME/spark*; do
        module=$(basename $folder)
        cp -vr $folder/target/scala-2.10/api $module
    done
done

cd $SCC_HOME
git checkout gh-pages
cp -r $OUTPUT/* $SCC_HOME/ApiDocs
