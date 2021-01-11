package io.github.leibnizhu.vmonitor.util

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

/**
 * @author Leibniz on 2020/12/31 3:45 PM
 */
object TimeUtil {
  private val timePattern = Pattern.compile("^(\\d+?)(ms|[smh])$")

  def parseTimeStrToMs(timeStr: String): Long = {
    val (time, unit) = TimeUtil.parseTimeStr(timeStr)
    unit.toMillis(time)
  }

  def parseTimeStr(timeStr: String): (Long, TimeUnit) = {
    val matched = timePattern.matcher(timeStr)
    if (matched.find()) {
      val time = matched.group(1).toLong
      val unit = matched.group(2) match {
        case "ms" => TimeUnit.MILLISECONDS
        case "s" => TimeUnit.SECONDS
        case "m" => TimeUnit.MINUTES
        case "h" => TimeUnit.HOURS
      }
      (time, unit)
    } else {
      throw new IllegalArgumentException("警告间隔时间配置格式不对:" + timeStr)
    }
  }

}
