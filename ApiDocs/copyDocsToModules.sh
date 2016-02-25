#!/bin/bash
for folder in ~/repos/spark-cassandra-connector/spark*; do module=$(basename $folder) ; cp -vr $folder/target/scala-2.10/api $module;  done;
