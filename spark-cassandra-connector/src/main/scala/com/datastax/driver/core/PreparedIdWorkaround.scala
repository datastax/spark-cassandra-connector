package com.datastax.driver.core

/**
  * PreparedId's fields are all invisible inside of it's structure
  * in the 3.X release of the Java Driver. We will have direct access
  * in 4.0 but for now we use this backdoor to get the resultSetMetadata
  * we want.
  */
object PreparedIdWorkaround {

  def getResultMetadata(preparedId: PreparedId) = {
    preparedId.resultSetMetadata
  }

}
