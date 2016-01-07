#!/bin/sh

echo "Running tests for Scala 2.10"
sbt/sbt clean package test it:test assembly
s210r="$?"

echo "Running tests for Scala 2.11"
sbt/sbt -Dscala-2.11=true clean package test it:test assembly
s211r="$?"

retval=0

if [ "$s210r" = "0" ]; then
  echo "Tests for Scala 2.10 succeeded"
else
  echo "Tests for Scala 2.10 failed"
  retval=1
fi

if [ "$s211r" = "0" ]; then
  echo "Tests for Scala 2.11 succeeded"
else
  echo "Tests for Scala 2.11 failed"
  retval=1
fi


exit $retval
