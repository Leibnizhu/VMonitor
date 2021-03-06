package io.github.leibnizhu.vmonitor

import com.codahale.metrics.SharedMetricRegistries
import io.github.leibnizhu.vmonitor.Constants.{ALERT_RULE_CONFIG_KEY, EVENTBUS_MONITOR_JSON_PARAM_METRIC_NAME, LISTEN_ADDRESS_CONFIG_KEY, MAIN_METRIC_NAME}
import io.github.leibnizhu.vmonitor.util.FutureUtil._
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.{AbstractVerticle, Promise}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer

/**
 * @author Leibniz on 2020/12/30 2:15 PM
 */
class EventCollectorVerticle extends AbstractVerticle {
  private val log = LoggerFactory.getLogger(getClass)
  private val metricRegistry = SharedMetricRegistries.getOrCreate(MAIN_METRIC_NAME)
  //  private var metricAlertRuleMap: Map[String, Array[AlertRule]] = _
  private val metricBufferMap: java.util.Map[String, ArrayBuffer[JsonObject]] = new ConcurrentHashMap[String, ArrayBuffer[JsonObject]]()

  override def start(startPromise: Promise[Void]): Unit = {
    log.info("======>启动:{},配置:{}", Array(getClass.getName, config()): _*)
    initRules(startPromise)
    initEventbus()
    startPromise.complete()
  }

  private def initRules(startPromise: Promise[Void]): Unit = {
    val ruleStr = config().getString(ALERT_RULE_CONFIG_KEY)
    if (StringUtils.isBlank(ruleStr)) {
      startPromise.fail("告警规则不能为空!配置key:" + ALERT_RULE_CONFIG_KEY)
    }
    //    metricAlertRuleMap = AlertRule.fromListJson(ruleStr).groupBy(_.metric.name)
    //每个规则的采样设置
    AlertRule.fromListJson(ruleStr).foreach(rule => {
      vertx.setPeriodic(rule.sampleIntervalMs, (tid: java.lang.Long) => {
        val metricBuffer = metricBufferMap.computeIfAbsent(rule.metric.name, (k: String) => new ArrayBuffer[JsonObject]())
        val sampledList = metricBuffer.toList
        metricBuffer.clear()
        //不同的metric，根据condition，有不同的统计策略
        rule.recordMetric(sampledList, metricRegistry)
      })
    })
  }

  private def initEventbus(): Unit = {
    val address = config().getString(LISTEN_ADDRESS_CONFIG_KEY)
    vertx.eventBus().consumer[JsonObject](address)
      .handler((msg: Message[JsonObject]) => {
        log.debug("{}地址接收到eventbus消息:{}", Array(address, msg.body()): _*)
        val jsonBody = msg.body()
        if (jsonBody == null || jsonBody.isEmpty) {
          log.error("监控记录请求内容为空")
        } else {
          val metricName = jsonBody.getString(EVENTBUS_MONITOR_JSON_PARAM_METRIC_NAME)
          if (metricName == null || metricName.isEmpty) {
            log.error("监听请求的指标名为空")
          } else {
            metricBufferMap.computeIfAbsent(metricName, (k: String) => new ArrayBuffer[JsonObject]()).append(jsonBody)
          }
        }
      })
  }

  override def stop(): Unit = {
    super.stop()
  }
}