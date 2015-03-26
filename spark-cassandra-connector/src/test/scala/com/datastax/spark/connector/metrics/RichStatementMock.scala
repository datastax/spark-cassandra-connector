package com.datastax.spark.connector.metrics

import com.datastax.spark.connector.writer.RichStatement

class RichStatementMock(val bytesCount: Int, val rowsCount: Int) extends RichStatement
