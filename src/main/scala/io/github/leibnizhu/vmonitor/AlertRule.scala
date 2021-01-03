package io.github.leibnizhu.vmonitor

import com.codahale.metrics.MetricRegistry
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{ObjectMapper, ObjectReader}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import io.github.leibnizhu.vmonitor.util.TimeUtil
import io.vertx.core.Future

/**
 * @author Leibniz on 2020/12/31 3:20 PM
 */
case class AlertRule(@JsonProperty(required = true) name: String,
                     @JsonProperty(required = true) metric: String,
                     @JsonProperty(required = true) period: CheckPeriod,
                     @JsonProperty(required = true) condition: AlertCondition,
                     @JsonProperty(required = true) alert: AlertMode) {
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

  def doAlert(env: String): Future[Unit] = {
    //TODO 根据 AlertMode 处理
    System.out.println("===============>" + env + "环境发出告警")
    Future.succeededFuture()
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
                       @JsonProperty(required = true) pend: String) {
}

case class AlertMode(@JsonProperty(required = true) method: String,
                     @JsonProperty(required = true) times: Int,
                     @JsonProperty(required = true) interval: String,
                     @JsonProperty(required = true) config: Map[String, String])

case class AlertCondition(@JsonProperty(required = true) method: String,
                          @JsonProperty(required = true) param: Map[String, String])