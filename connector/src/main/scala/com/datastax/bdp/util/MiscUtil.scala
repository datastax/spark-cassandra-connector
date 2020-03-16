/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.util

import scala.reflect.runtime.universe

object MiscUtil {
  def objectOrClassName(o: AnyRef): String = {
    val mirror = universe.runtimeMirror(o.getClass.getClassLoader)
    mirror.reflect(o).symbol.asClass.fullName
  }
}
