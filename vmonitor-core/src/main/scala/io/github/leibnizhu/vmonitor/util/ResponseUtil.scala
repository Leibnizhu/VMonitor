package io.github.leibnizhu.vmonitor.util

import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

/**
 * @author Leibniz on 2020/10/28 11:59 AM
 */
object ResponseUtil {
  private val log = LoggerFactory.getLogger(getClass)

  def successResponse(result: Any, costTime: Long): JsonObject =
    new JsonObject().
      put("status", "success")
      .put("results", result)
      .put("cost", costTime)

  def failResponse(cause: Throwable, costTime: Long): JsonObject =
    new JsonObject()
      .put("status", "error")
      .put("message", s"${cause.getClass.getName}:${cause.getMessage}")
      .put("cost", costTime)

  def failResponse(errMsg: String, costTime: Long): JsonObject =
    new JsonObject()
      .put("status", "error")
      .put("message", errMsg)
      .put("cost", costTime)

  def failResponseWithMsg(errMsg: String, startTime: Long, cause: Throwable): JsonObject = {
    val costTime = System.currentTimeMillis() - startTime
    log.error(s"${errMsg}失败, 耗时${costTime}毫秒:${cause.getMessage}", cause)
    failResponse(cause, costTime)
  }
}
