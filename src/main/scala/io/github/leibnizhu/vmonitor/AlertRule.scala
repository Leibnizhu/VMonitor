package io.github.leibnizhu.vmonitor

import com.codahale.metrics.MetricRegistry
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{ObjectMapper, ObjectReader}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, JsonScalaEnumeration, ScalaObjectMapper}
import io.github.leibnizhu.vmonitor.AlertMethod.{AlertMethod, Log, WecomBot}
import io.github.leibnizhu.vmonitor.Constants.SEND_WECOM_BOT_EVENTBUS_ADDR
import io.github.leibnizhu.vmonitor.util.TimeUtil
import io.github.leibnizhu.vmonitor.wecom.message.MarkdownMessage
import io.github.leibnizhu.vmonitor.wecom.message.MarkdownMessage.MarkdownBuilder
import io.vertx.core.{Future, Vertx}
import org.slf4j.LoggerFactory

/**
 * @author Leibniz on 2020/12/31 3:20 PM
 */
case class AlertRule(@JsonProperty(required = true) name: String,
                     @JsonProperty(required = true) metric: String,
                     @JsonProperty(required = true) period: CheckPeriod,
                     @JsonProperty(required = true) condition: AlertCondition,
                     @JsonProperty(required = true) alert: AlertMode) {
  private val log = LoggerFactory.getLogger(getClass)
  lazy val checkPeriodMs: Long = TimeUtil.parseTimeStrToMs(period.every)
  lazy val pendingMs: Long = TimeUtil.parseTimeStrToMs(period.pend)
  lazy val alertIntervalMs: Long = TimeUtil.parseTimeStrToMs(alert.interval)
  lazy val alertCounterName = s"$name:alert:counter" //当前连续报警多少次
  lazy val alertFsmMapName = s"$name:alert:fsm:map" //fsm保存上下文的分布式map名
  lazy val alertFsmLockName = s"$name:alert:fsm:lock" //fsm的处理锁

  def satisfiedAlertCondition(metricRegistry: MetricRegistry): Boolean = {
    //TODO 根据 AlertCondition 处理
    metricRegistry.counter(metric).getCount > 3
  }

  def doAlert(env: String, vertx: Vertx): Future[Unit] = {
    //根据 AlertMode 处理
    alert.method match {
      case WecomBot =>
        val token = alert.config("token")
        val customMsg = alert.config("message")
        //TODO 完善报警信息，包括具体报错情况
        val messageBuilder = new MarkdownBuilder()
          .text(env).text("环境的\"").text(name).text("\"规则发出告警:").newLine()
        if (customMsg != null) {
          messageBuilder.text(customMsg)
        }
        val wecomMsgJson = MarkdownMessage(token, messageBuilder.toMarkdownString).serializeToJsonObject()
        vertx.eventBus().request(SEND_WECOM_BOT_EVENTBUS_ADDR, wecomMsgJson).mapEmpty()
      case Log =>
        log.warn("===============>{}环境的{}规则发出告警", Array(env, name): _*)
        Future.succeededFuture()
    }
  }
}

object AlertRule {
  protected val mapper: ObjectMapper = (new ObjectMapper() with ScalaObjectMapper)
    .registerModule(DefaultScalaModule).configure(JsonParser.Feature.ALLOW_COMMENTS, true)
  protected val objectReader: ObjectReader = mapper.readerFor(classOf[AlertRule])
  protected val objectListReader: ObjectReader = mapper.readerFor(classOf[Array[AlertRule]])

  def apply(json: String): AlertRule = objectReader.readValue[AlertRule](json)

  def fromListJson(listJson: String): Array[AlertRule] = objectListReader.readValue(listJson)
}

case class CheckPeriod(@JsonProperty(required = true) every: String,
                       @JsonProperty(required = true) pend: String)

case class AlertMode(@JsonProperty(required = true) @JsonScalaEnumeration(classOf[AlertMethodType]) method: AlertMethod,
                     @JsonProperty(required = true) times: Int,
                     @JsonProperty(required = true) interval: String,
                     @JsonProperty(required = true) config: Map[String, String])

case class AlertCondition(@JsonProperty(required = true) method: String,
                          @JsonProperty(required = true) param: Map[String, String])

object AlertMethod extends Enumeration {
  type AlertMethod = Value
  val WecomBot, Log = Value
}

class AlertMethodType extends TypeReference[AlertMethod.type]
