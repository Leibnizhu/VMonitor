package io.github.leibnizhu.vmonitor

import com.codahale.metrics.MetricRegistry.MetricSupplier
import com.codahale.metrics._
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{ObjectMapper, ObjectReader}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, JsonScalaEnumeration, ScalaObjectMapper}
import io.github.leibnizhu.vmonitor.AlertMethod.{AlertMethod, Log, WecomBot}
import io.github.leibnizhu.vmonitor.Constants.SEND_WECOM_BOT_EVENTBUS_ADDR
import io.github.leibnizhu.vmonitor.MetricTargetMethod.{AsOne, GetFromJson, MetricTargetMethod}
import io.github.leibnizhu.vmonitor.util.TimeUtil
import io.github.leibnizhu.vmonitor.wecom.message.MarkdownMessage
import io.github.leibnizhu.vmonitor.wecom.message.MarkdownMessage.MarkdownBuilder
import io.vertx.core.json.JsonObject
import io.vertx.core.{Future, Vertx}
import org.slf4j.LoggerFactory

import scala.util.Try

/**
 * @author Leibniz on 2020/12/31 3:20 PM
 */
case class AlertRule(@JsonProperty(required = true) name: String,
                     @JsonProperty(required = true) metric: MonitorMetric,
                     @JsonProperty(required = true) period: CheckPeriod,
                     @JsonProperty(required = true) condition: AlertCondition,
                     @JsonProperty(required = true) alert: AlertMode) {
  lazy val checkPeriodMs: Long = TimeUtil.parseTimeStrToMs(period.every)
  lazy val pendingMs: Long = TimeUtil.parseTimeStrToMs(period.pend)
  lazy val alertIntervalMs: Long = TimeUtil.parseTimeStrToMs(alert.interval)
  lazy val alertCounterName = s"$name:alert:counter" //当前连续报警多少次
  lazy val alertFsmMapName = s"$name:alert:fsm:map" //fsm保存上下文的分布式map名
  lazy val alertFsmLockName = s"$name:alert:fsm:lock" //fsm的处理锁

  // 初始化指标Histogram
  def initMetric(metricRegistry: MetricRegistry): Unit = {
    val (time, unit) = TimeUtil.parseTimeStr(condition.last)
    metricRegistry.histogram(metric.name, new MetricSupplier[Histogram]() {
      override def newMetric(): Histogram =
        new Histogram(if (time == 0) new UniformReservoir() else new SlidingTimeWindowReservoir(time, unit))
    })
  }

  // 根据 AlertCondition 处理,不同的metric，根据condition，有不同的统计策略
  def recordMetric(json: JsonObject, metricRegistry: MetricRegistry): Unit =
    metricRegistry.histogram(metric.name).update(metric.metricTarget(json))

  // 根据 AlertCondition 处理
  def judgeAlertCond(metricRegistry: MetricRegistry): Boolean =
    condition.compare(condition.metricValue(metricRegistry.histogram(metric.name).getSnapshot))

  //根据 AlertMode 处理
  def doAlert(alerting: Boolean, env: String, vertx: Vertx): Future[Unit] = alert.method match {
    case WecomBot => WecomBot.doAlert(this, alerting, env, vertx)
    case Log => Log.doAlert(this, alerting, env, vertx)
  }
}

object AlertRule {
  protected lazy val mapper: ObjectMapper = (new ObjectMapper() with ScalaObjectMapper)
    .registerModule(DefaultScalaModule).configure(JsonParser.Feature.ALLOW_COMMENTS, true)
  protected lazy val objectReader: ObjectReader = mapper.readerFor(classOf[AlertRule])
  protected lazy val objectListReader: ObjectReader = mapper.readerFor(classOf[Array[AlertRule]])

  def apply(json: String): AlertRule = objectReader.readValue[AlertRule](json)

  def fromListJson(listJson: String): Array[AlertRule] = objectListReader.readValue(listJson)
}

case class MonitorMetric(@JsonProperty(required = true) name: String,
                         @JsonProperty(required = true) @JsonScalaEnumeration(classOf[MetricTargetMethodType]) targetMethod: MetricTargetMethod,
                         @JsonProperty(required = true) jsonKey: String) {
  def metricTarget(json: JsonObject): Long = targetMethod match {
    case GetFromJson => Try[Long](json.getLong(jsonKey)) recoverWith {
      case ClassCastException => Try(json.getString(jsonKey).toDouble.longValue())
    } getOrElse 0
    case AsOne => 1
  }
}

object MetricTargetMethod extends Enumeration {
  type MetricTargetMethod = Value
  val GetFromJson, AsOne = Value
}

class MetricTargetMethodType extends TypeReference[MetricTargetMethod.type]

case class CheckPeriod(@JsonProperty(required = true) every: String,
                       @JsonProperty(required = true) pend: String)

case class AlertMode(@JsonProperty(required = true) @JsonScalaEnumeration(classOf[AlertMethodType]) method: AlertMethod,
                     @JsonProperty(required = true) times: Int,
                     @JsonProperty(required = true) interval: String,
                     @JsonProperty(required = true) config: Map[String, String])

case class AlertCondition(@JsonProperty(required = true) method: String,
                          @JsonProperty(required = true) last: String,
                          @JsonProperty(required = true) op: String,
                          @JsonProperty(required = true) threshold: Long) {
  private val log = LoggerFactory.getLogger(getClass)

  def metricValue(snapshot: Snapshot): Double = method.toLowerCase match {
    case "min" => snapshot.getMin
    case "max" => snapshot.getMax
    case "avg" => snapshot.getMean //平均数
    case "median" => snapshot.getMedian //中位数
    case "stddev" => snapshot.getStdDev //标准差
    case "count" => snapshot.size()
    case "sum" => snapshot.getValues.sum
    case _ =>
      log.error("监控规则中出现了不支持的指标计算方法:" + method)
      0
  }

  def compare(curValue: Double): Boolean = op match {
    case ">" => curValue > threshold
    case ">=" => curValue >= threshold
    case "<" => curValue < threshold
    case "<=" => curValue <= threshold
    case "=" => curValue == threshold
    case _ =>
      log.error("监控规则中出现了不支持的比较符:" + op)
      false
  }
}

object AlertMethod extends Enumeration {
  private val log = LoggerFactory.getLogger(getClass)
  type AlertMethod = Value

  abstract class AlertMethodValue() extends Val() {
    def doAlert(rule: AlertRule, alerting: Boolean, env: String, vertx: Vertx): Future[Unit]
  }

  val WecomBot: AlertMethodValue = new AlertMethodValue() {
    override def doAlert(rule: AlertRule, alerting: Boolean, env: String, vertx: Vertx): Future[Unit] = {
      val token = rule.alert.config("token")
      val customMsg = rule.alert.config("message")
      val keyColor = if (alerting) "info" else "warning"
      //TODO 完善报警信息，包括具体报错情况
      val messageBuilder = new MarkdownBuilder()
        .colored(keyColor, s"警报${if (alerting) "发出" else "解除"}").newLine()
        .quoted().text("环境:").text(env).newLine()
        .quoted().text("触发规则:").text(rule.name).newLine()
      if (customMsg != null) {
        messageBuilder.quoted().text(customMsg)
      }
      val wecomMsgJson = MarkdownMessage(token, messageBuilder.toMarkdownString).serializeToJsonObject()
      vertx.eventBus().request(SEND_WECOM_BOT_EVENTBUS_ADDR, wecomMsgJson).mapEmpty()
    }
  }
  val Log: AlertMethodValue = new AlertMethodValue() {
    override def doAlert(rule: AlertRule, alerting: Boolean, env: String, vertx: Vertx): Future[Unit] = {
      log.warn("===============>{}环境的{}规则{}告警", Array(env, rule.name, if (alerting) "发出" else "解除"): _*)
      Future.succeededFuture()
    }
  }
}

class AlertMethodType extends TypeReference[AlertMethod.type]
