package com.datastax.spark.connector.metrics

import org.apache.spark.{SparkConf, SparkEnv}

class SparkEnvMock(conf: SparkConf) extends SparkEnv(
  null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, conf
)
