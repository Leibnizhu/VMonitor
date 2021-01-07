package io.github.leibnizhu.vmonitor.util

import java.lang.management.ManagementFactory
import java.net.{InetAddress, UnknownHostException}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

/**
 * @author Leibniz on 2021/01/7 4:30 PM
 */
object SystemUtil {
  def hostName(): String =
    if (System.getenv("COMPUTERNAME") != null) System.getenv("COMPUTERNAME")
    else try InetAddress.getLocalHost.getHostName
    catch {
      case uhe: UnknownHostException =>
        val host = uhe.getMessage // host = "hostname: hostname"
        if (host != null) {
          val colon = host.indexOf(':')
          if (colon > 0) return host.substring(0, colon)
        }
        "UnknownHost"
    }

  def currentPid(): Long = {
    val processName = ManagementFactory.getRuntimeMXBean.getName
    processName.split("@")(0).toLong
  }

  private val tl: ThreadLocal[DateFormat] = ThreadLocal.withInitial(() => new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))

  def currentTime(): String = {
    tl.get().format(new Date())
  }
}
