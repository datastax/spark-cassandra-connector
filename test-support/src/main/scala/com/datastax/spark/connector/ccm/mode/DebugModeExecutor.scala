package com.datastax.spark.connector.ccm.mode

import java.nio.file.{Files, Path, Paths}

import com.datastax.spark.connector.ccm.CcmConfig
import org.slf4j.{Logger, LoggerFactory}

private[ccm] class DebugModeExecutor(val config: CcmConfig) extends DefaultExecutor {

  private val logger: Logger = LoggerFactory.getLogger(classOf[StandardModeExecutor])

  private val Cwd = Paths.get("").toAbsolutePath().toString();

  override val dir: Path = {
    sys.env.get("PRESERVE_LOGS") match {
      case Some(dir) =>
        val subPath = s"$Cwd/$dir/ccm_${config.ipPrefix
          .replace(".","_")
          .stripSuffix("_")}"

        val path = Files.createDirectories(Paths.get(subPath))
        logger.debug(s"Preserving CCM Install Directory at [$path]. It will not be removed")
        logger.debug(s"Checking directory exists [${Files.exists(path)}]")
        path
      case None =>
        val tmp = Files.createTempDirectory("ccm")
        tmp.toFile.deleteOnExit()
        tmp
    }
  }

  // stop nodes, don't remove logs
  override def remove(): Unit = {
     execute("stop")
  }

}
