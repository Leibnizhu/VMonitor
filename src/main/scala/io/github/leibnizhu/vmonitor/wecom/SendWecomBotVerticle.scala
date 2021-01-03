package io.github.leibnizhu.vmonitor.wecom

import io.github.leibnizhu.vmonitor.Constants
import io.github.leibnizhu.vmonitor.Constants.SEND_WECOM_BOT_EVENTBUS_ADDR
import io.github.leibnizhu.vmonitor.util.ResponseUtil
import io.github.leibnizhu.vmonitor.wecom.message.MessageContent
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.{AbstractVerticle, AsyncResult, Handler}
import io.vertx.ext.web.client.{HttpResponse, WebClient, WebClientOptions}
import org.slf4j.LoggerFactory

import scala.util.Try

/**
 * @author Leibniz on 2020/11/25 8:38 AM
 */
class SendWecomBotVerticle extends AbstractVerticle {
  private val log = LoggerFactory.getLogger(getClass)
  private var client: WebClient = _

  override def start(): Unit = {
    this.client = WebClient.create(vertx, new WebClientOptions().setKeepAlive(true))
    registerEventBus()
  }

  private def registerEventBus(): Unit = {
    vertx.eventBus().consumer(SEND_WECOM_BOT_EVENTBUS_ADDR)
      .handler((eventBusMsg: Message[JsonObject]) =>
        Try({
          val msgContent = MessageContent.deserializeFromJsonObject(eventBusMsg.body())
          val reqJson = msgContent.wholeJson()
          if (log.isDebugEnabled) {
            log.debug("构造企业微信机器人请求:{}", reqJson)
          }
          sendWecomBotMessage(msgContent.token(), reqJson, eventBusMsg)
        }) recover {
          case e: Throwable => eventBusMsg.reply(ResponseUtil.failResponse(e, 0L))
        }
      )
  }

  private def sendWecomBotMessage(token: String, reqJson: JsonObject, eventBusMsg: Message[JsonObject]): Unit = {
    val startTime = System.currentTimeMillis()
    doSendReq(reqJson, token, sendAr => {
      val costTime = System.currentTimeMillis() - startTime
      val responseJson = if (sendAr.succeeded()) {
        val responseStr = sendAr.result().bodyAsString()
        log.info("发送企业微信机器人请求成功, 耗时{}毫秒,响应:{}", Array(Long.box(costTime), responseStr): _*)
        val result = new JsonObject().put("message", s"发送企业微信机器人请求成功, 耗时${costTime}毫秒").put("wecomResponse", responseStr)
        ResponseUtil.successResponse(result, costTime)
      } else {
        ResponseUtil.failResponseWithMsg("发送企业微信机器人请求", startTime, sendAr.cause())
      }
      eventBusMsg.reply(responseJson)
    })
  }

  def doSendReq(req: JsonObject, token: String, handler: Handler[AsyncResult[HttpResponse[Buffer]]]): Unit = {
    client
      .postAbs(Constants.WECOM_BOT_API_URL + token)
      .sendJsonObject(req, handler)
  }
}