package io.github.leibnizhu.vmonitor.wecom.message

import io.github.leibnizhu.vmonitor.Constants._
import io.github.leibnizhu.vmonitor.wecom.message.MessageType.MessageType
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
 * @author Leibniz on 2020/10/28 2:21 PM
 */
trait MessageContent {
  def msgType(): MessageType

  def token(): String

  def wholeJson(): JsonObject = {
    val reqJson = new JsonObject()
    val msgTypeStr = MessageType.msgTypeName(msgType())
    reqJson.put("msgtype", msgTypeStr)
    reqJson.put(msgTypeStr, contentJsonObject())
    reqJson
  }

  def contentJsonObject(): JsonObject

  def serializeToJsonObject(): JsonObject = serializeToJsonObject(new JsonObject()
    .put(EVENTBUS_WECOM_BOT_JSON_PARAM_TYPE, MessageType.msgTypeName(msgType()))
    .put(EVENTBUS_WECOM_BOT_JSON_PARAM_TOKEN, token()))

  def serializeToJsonObject(json: JsonObject): JsonObject
}

object MessageContent {
  private val log = LoggerFactory.getLogger(getClass)

  def deserializeFromJsonObject(json: JsonObject): MessageContent = {
    val token = json.getString(EVENTBUS_WECOM_BOT_JSON_PARAM_TOKEN)
    val content = json.getString(EVENTBUS_WECOM_BOT_JSON_PARAM_CONTENT)
    json.getString(EVENTBUS_WECOM_BOT_JSON_PARAM_TYPE) match {
      case EVENTBUS_WECOM_BOT_JSON_PARAM_TYPE_MARKDOWN =>
        MarkdownMessage(token, content)
      case EVENTBUS_WECOM_BOT_JSON_PARAM_TYPE_TEXT =>
        if (json.getJsonArray(EVENTBUS_WECOM_BOT_JSON_PARAM_MENTION_LIST) != null) {
          val mentionListBuf = new ListBuffer[String]
          json.getJsonArray(EVENTBUS_WECOM_BOT_JSON_PARAM_MENTION_LIST).forEach(m => mentionListBuf += m.toString)
          TextMessage(token, content, mentionListBuf.toList)
        } else {
          TextMessage(token, content)
        }
      case _ => throw new IllegalArgumentException("暂不支持的消息:" + json)
    }
  }
}
