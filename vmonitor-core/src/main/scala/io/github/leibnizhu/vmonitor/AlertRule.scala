package io.github.leibnizhu.vmonitor

import com.codahale.metrics.MetricRegistry.MetricSupplier
import com.codahale.metrics._
import com.fasterxml.jackson.annotation.JsonProperty
import io.github.leibnizhu.vmonitor.AlertRule.{log, safeToDouble, safeToLong, supportAlertAggFunc, supportAlertOp, supportMetricAggFunc}
import io.github.leibnizhu.vmonitor.Constants.SEND_WECOM_BOT_EVENTBUS_ADDR
import io.github.leibnizhu.vmonitor.util.{SystemUtil, TimeUtil}
import io.github.leibnizhu.vmonitor.wecom.message.MarkdownMessage
import io.github.leibnizhu.vmonitor.wecom.message.MarkdownMessage.MarkdownBuilder
import io.vertx.core.json.JsonObject
import io.vertx.core.{Future, Vertx}
import org.apache.commons.lang3.StringUtils
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

import java.lang
import java.util.regex.Pattern
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
  lazy val sampleIntervalMs: Long = TimeUtil.parseTimeStrToMs(metric.sampleInterval)
  lazy val alertCounterName = s"$name:alert:counter" //当前连续报警多少次
  lazy val alertFsmMapName = s"$name:alert:fsm:map" //fsm保存上下文的分布式map名
  lazy val alertFsmLockName = s"$name:alert:fsm:lock" //fsm的处理锁
  lazy val histogramName = s"${metric.name}.$name"

  def validate(): AlertRule = {
    metric.validate()
    condition.validate()
    this
  }

  // 初始化指标Histogram
  def initMetric(metricRegistry: MetricRegistry): Unit = {
    val (time, unit) = TimeUtil.parseTimeStr(condition.last)
    metricRegistry.histogram(histogramName, new MetricSupplier[Histogram]() {
      override def newMetric(): Histogram =
        new Histogram(if (time == 0) new UniformReservoir() else new SlidingTimeWindowReservoir(time, unit))
    })
  }

  // 根据 AlertCondition 处理,不同的metric，根据condition，有不同的统计策略
  def recordMetric(sampledList: List[JsonObject], metricRegistry: MetricRegistry): Unit = {
    val sampleValue = metric.sample(sampledList)
    if (log.isDebugEnabled) {
      log.debug(s"======>'${metric.name}'指标在${name}规则采样源:${sampledList}使用规则:${metric}最终采样值:$sampleValue")
    }
    metricRegistry.histogram(histogramName).update(sampleValue)
  }

  // 根据 AlertCondition 处理
  def judgeAlertCond(metricRegistry: MetricRegistry): Boolean = {
    val snapshot = metricRegistry.histogram(histogramName).getSnapshot
    if (log.isDebugEnabled) {
      log.debug(s"======>'${metric.name}'指标在${name}规则的监控窗口:${snapshot.getValues.mkString("Array(", ", ", ")")}")
    }
    val satisfied = condition.compare(condition.metricValue(snapshot))
    if (log.isDebugEnabled) {
      log.debug(s"======>'${metric.name}'指标在${name}规则的$condition${if (satisfied) "满足" else "暂不满足"}")
    }
    satisfied
  }

  //根据 AlertMode 处理
  def doAlert(alerting: Boolean, env: String, vertx: Vertx): Future[Unit] = alert.doAlert(this, alerting, env, vertx)
}

object AlertRule {
  val log: Logger = LoggerFactory.getLogger(getClass)
  val supportMetricAggFunc = Set("uniquecount", "count", "min", "max", "sum", "avg")
  val supportAlertAggFunc = Set("uniquecount", "count", "min", "max", "sum", "avg", "stddev", "median")
  val supportAlertOp = Set(">", ">=", "<", "<=", "=")

  implicit val formats: DefaultFormats.type = DefaultFormats

  def apply(json: String): AlertRule = parse(json).extract[AlertRule]

  def fromListJson(listJson: String): Array[AlertRule] = parse(listJson).extract[Array[AlertRule]]

  def allWecomBotToken(rules: Iterable[AlertRule]): Iterable[String] =
    rules
      .filter(rule => rule.alert.method == "WecomBot")
      .map(rule => Option(rule.alert).map(_.config).flatMap(_.get("token")).orNull)
      .filter(_ != null)
      .toList.distinct

  def safeToLong(v: Any): java.lang.Long = v match {
    case l: Long => l
    case i: Int => i
    case d: Double => d.toLong
    case f: Float => f.toLong
    case _ => Try(Long.box(v.toString.toLong)).getOrElse(null)
  }

  def safeToDouble(v: Any): java.lang.Double = v match {
    case l: Long => l.toDouble
    case i: Int => i.toDouble
    case d: Double => d
    case f: Float => f.toDouble
    case _ => Try(Double.box(v.toString.toDouble)).getOrElse(null)
  }
}

case class MonitorMetric(@JsonProperty(required = true) name: String,
                         @JsonProperty(required = false) filter: List[FilterCondition] = null,
                         @JsonProperty(required = false) groupField: List[String] = null,
                         @JsonProperty(required = false) groupAggFunc: String = null,
                         @JsonProperty(required = false) groupAggField: String = null,
                         @JsonProperty(required = true) sampleInterval: String,
                         @JsonProperty(required = false) sampleAggField: String = null,
                         @JsonProperty(required = true) sampleAggFunc: String) {
  def validate(): Unit = {
    if (groupField != null && groupField.nonEmpty) {
      if (StringUtils.isAnyBlank(groupAggFunc, groupAggField)) {
        throw new IllegalArgumentException("指标采样设置了groupField时，必须设置groupAggFunc及groupAggField")
      } else if (!supportMetricAggFunc.contains(groupAggFunc.toLowerCase)) {
        throw new IllegalArgumentException("指标采样设置了不支持的groupAggFunc:" + groupAggFunc)
      }
    } else {
      if (StringUtils.isBlank(sampleAggField)) {
        throw new IllegalArgumentException("指标采样未设置groupField时，必须设置sampleAggField")
      } else if (!supportMetricAggFunc.contains(sampleAggFunc.toLowerCase)) {
        throw new IllegalArgumentException("指标采样设置了不支持的sampleAggFunc:" + sampleAggFunc)
      }
    }
  }

  def sample(sampledList: List[JsonObject]): Long = {
    val filteredSampleList = if (filter != null) sampledList.filter(json => filter.forall(_.satisfied(json))) else sampledList
    if (groupField != null && groupField.nonEmpty) {
      val groupedSampleList = filteredSampleList
        .groupBy(json => groupField.map(json.getValue))
        .mapValues(jsonList => doAgg(groupAggFunc, jsonList.map(_.getValue(groupAggField)).filter(_ != null)))
        .values
      doAgg(sampleAggFunc, groupedSampleList)
    } else {
      doAgg(sampleAggFunc, filteredSampleList.map(_.getValue(sampleAggField)).filter(_ != null))
    }
  }

  private def doAgg(aggFunc: String, aggValueList: Iterable[Any]): Long = aggFunc.toLowerCase match {
    case "uniquecount" => aggValueList.toList.distinct.size
    case "count" => aggValueList.size
    case "min" =>
      val points = aggValueList.map(safeToLong).filter(_ != null)
      if (points.nonEmpty) points.min else 0
    case "max" =>
      val points = aggValueList.map(safeToLong).filter(_ != null)
      if (points.nonEmpty) points.max else 0
    case "sum" => aggValueList.map(safeToLong).filter(_ != null).map(Long.unbox(_)).sum
    case "avg" => if (aggValueList.isEmpty) 0 else
      aggValueList.map(safeToLong).filter(_ != null).map(Long.unbox(_)).sum / aggValueList.size
    case _ =>
      log.error("监控规则中出现了不支持的采样聚合函数:" + aggFunc)
      0
  }

  override def toString: String =
    s"每${sampleInterval}采样" +
      s"${if (filter != null && filter.nonEmpty) s"按${filter}过滤," else ""}" +
      s"${if (groupField != null && groupField.nonEmpty) s"按${groupField}分组后$groupAggFunc($groupAggField),最后所有分组" else ""}" +
      s"$sampleAggFunc($sampleAggField)"
}

case class FilterCondition(@JsonProperty(required = true) key: String,
                           @JsonProperty(required = true) op: String,
                           @JsonProperty(required = true) target: String) {
  lazy val targetDouble: lang.Double = safeToDouble(target)
  lazy val targetReg: Pattern = Pattern.compile(target)

  def satisfied(json: JsonObject): Boolean = {
    val value = json.getValue(key)
    val doubleValue = safeToLong(value)
    op match {
      case "=" => value != null && value.equals(target)
      case ">" => doubleValue != null && doubleValue > targetDouble
      case ">=" => doubleValue != null && doubleValue >= targetDouble
      case "<" => doubleValue != null && doubleValue < targetDouble
      case "<=" => doubleValue != null && doubleValue <= targetDouble
      case "reg" => value != null && targetReg.matcher(value.toString).find()
      case _ =>
        log.error("监控规则中出现了不支持的比较符:" + op)
        false
    }
  }
}

case class CheckPeriod(@JsonProperty(required = true) every: String,
                       @JsonProperty(required = true) pend: String)

case class AlertMode(@JsonProperty(required = true) method: String,
                     @JsonProperty(required = true) times: Int,
                     @JsonProperty(required = true) interval: String,
                     @JsonProperty(required = true) config: Map[String, String]) {
  def doAlert(rule: AlertRule, alerting: Boolean, env: String, vertx: Vertx): Future[Unit] = method match {
    case "WecomBot" =>
      val token = rule.alert.config("token")
      val customMsg = rule.alert.config("message")
      val keyColor = if (alerting) "warning" else "info"
      //TODO 完善报警信息，包括具体报错情况
      val messageBuilder = new MarkdownBuilder()
        .colored(keyColor, s"VMonitor警报${if (alerting) "发出" else "解除"}").newLine()
        .quoted().text("环境:").text(env).newLine()
        .quoted().text("时刻:").text(SystemUtil.currentTime()).newLine()
        .quoted().text("触发规则:").text(rule.name).newLine()
      if (customMsg != null) {
        messageBuilder.quoted().text(customMsg)
      }
      val wecomMsgJson = MarkdownMessage(token, messageBuilder.toMarkdownString).serializeToJsonObject()
      vertx.eventBus().request(SEND_WECOM_BOT_EVENTBUS_ADDR, wecomMsgJson).mapEmpty()
    case "Log" =>
      log.warn("===============>{}环境的{}规则{}告警", Array(env, rule.name, if (alerting) "发出" else "解除"): _*)
      Future.succeededFuture()
  }
}

case class AlertCondition(@JsonProperty(required = true) method: String,
                          @JsonProperty(required = true) last: String,
                          @JsonProperty(required = true) op: String,
                          @JsonProperty(required = true) threshold: Long) {
  private val log = LoggerFactory.getLogger(getClass)

  def validate(): Unit = {
    if (!supportAlertAggFunc.contains(method.toLowerCase)) {
      throw new IllegalArgumentException("告警条件设置了不支持的聚合method:" + method)
    }
    if (!supportAlertOp.contains(op)) {
      throw new IllegalArgumentException("告警条件设置了不支持的聚合结果比较符(op):" + op)
    }
  }

  def metricValue(snapshot: Snapshot): Double = method.toLowerCase match {
    case "min" => snapshot.getMin
    case "max" => snapshot.getMax
    case "avg" => snapshot.getMean //平均数
    case "median" => snapshot.getMedian //中位数
    case "stddev" => snapshot.getStdDev //标准差
    case "count" => snapshot.size()
    case "uniquecount" => snapshot.getValues.distinct.length
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

  override def toString: String = s"告警条件${last}内,$method()$op$threshold"
}
