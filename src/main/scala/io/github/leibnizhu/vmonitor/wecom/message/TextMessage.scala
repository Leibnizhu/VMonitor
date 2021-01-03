package io.github.leibnizhu.vmonitor.wecom.message

import io.github.leibnizhu.vmonitor.Constants._
import io.github.leibnizhu.vmonitor.wecom.message.MessageType.MessageType
import io.vertx.core.json.JsonObject

import scala.collection.JavaConverters._

/**
 * @author Leibniz on 2020/10/28 2:48 PM
 */
case class TextMessage(apiToken: String, text: String, mentionedList: List[String] = List()) extends MessageContent {
  override def msgType(): MessageType = MessageType.Text

  override def contentJsonObject(): JsonObject = {
    val json = new JsonObject().put("content", text)
    if (mentionedList != null && mentionedList.nonEmpty) {
      json.put("mentioned_mobile_list", mentionedList.asJava)
    }
    json
  }

  override def token(): String = apiToken

  override def serializeToJsonObject(json: JsonObject): JsonObject = json
    .put(EVENTBUS_WECOM_BOT_JSON_PARAM_CONTENT, text)
    .put(EVENTBUS_WECOM_BOT_JSON_PARAM_MENTION_LIST, if (mentionedList == null) null else mentionedList.asJava)
}
