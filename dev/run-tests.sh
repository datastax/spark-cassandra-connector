#!/bin/sh

echo "Running tests for Scala 2.11"
sbt/sbt clean package test it:test assembly
s211r="$?"

echo "Running tests for Scala 2.12"
sbt/sbt -Dscala-2.12=true clean package test it:test assembly
s211r="$?"

retval=0

if [ "$s211r" = "0" ]; then
  echo "Tests for Scala 2.11 succeeded"
else
  echo "Tests for Scala 2.11 failed"
  retval=1
fi

if [ "$s212r" = "0" ]; then
  echo "Tests for Scala 2.12 succeeded"
else
  echo "Tests for Scala 2.12 failed"
  retval=1
fi


exit $retval
