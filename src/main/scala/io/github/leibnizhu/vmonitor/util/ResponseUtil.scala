package io.github.leibnizhu.vmonitor.util

import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonObject
import io.vertx.core.{Future, Handler}
import io.vertx.ext.web.RoutingContext
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

  def emptyTokenError: Handler[RoutingContext] = rc => {
    rc.response.putHeader("content-type", "application/json;charset=UTF-8")
      .end(failResponse("企业微信机器人token不能为空!", 0).toString)
  }

  def failResponseWithMsg(errMsg: String, startTime: Long, cause: Throwable): JsonObject = {
    val costTime = System.currentTimeMillis() - startTime
    log.error(s"${errMsg}失败, 耗时${costTime}毫秒:${cause.getMessage}", cause)
    failResponse(cause, costTime)
  }

  def handlerException(errMsg: String, startTime: Long, response: HttpServerResponse, cause: Throwable): Future[Void] = {
    response.end(failResponseWithMsg(errMsg, startTime, cause).toString)
  }
}
