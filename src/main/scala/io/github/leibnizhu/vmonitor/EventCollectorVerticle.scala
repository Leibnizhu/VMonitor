package io.github.leibnizhu.vmonitor

import com.codahale.metrics.SharedMetricRegistries
import io.github.leibnizhu.vmonitor.Constants.{ALERT_RULE_CONFIG_KEY, EVENTBUS_MONITOR_JSON_PARAM_METRIC_NAME, LISTEN_ADDRESS_CONFIG_KEY, MAIN_METRIC_NAME}
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.core.{AbstractVerticle, Promise}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

/**
 * @author Leibniz on 2020/12/30 2:15 PM
 */
class EventCollectorVerticle extends AbstractVerticle {
  private val log = LoggerFactory.getLogger(getClass)
  private val metricRegistry = SharedMetricRegistries.getOrCreate(MAIN_METRIC_NAME)
  private var metricAlertRuleMap: Map[String, Array[AlertRule]] = _

  override def start(startPromise: Promise[Void]): Unit = {
    log.info("======>启动:{},配置:{}", Array(getClass.getName, config()): _*)
    initRules(startPromise)
    initEventbus()
    startPromise.complete()
  }

  private def initEventbus(): Unit = {
    val address = config().getString(LISTEN_ADDRESS_CONFIG_KEY)
    vertx.eventBus().consumer(address)
      .handler((msg: Message[JsonObject]) => {
        log.debug("{}地址接收到eventbus消息:{}", Array(address, msg.body()): _*)
        val jsonBody = msg.body()
        if (jsonBody == null || jsonBody.isEmpty) {
          msg.reply(new JsonObject().put("errMsg", "监控记录请求内容为空"))
        } else {
          val metricName = jsonBody.getString(EVENTBUS_MONITOR_JSON_PARAM_METRIC_NAME)
          if (metricName == null || metricName.isEmpty) {
            msg.reply(new JsonObject().put("errMsg", "监听请求的指标名为空"))
          } else {
            //TODO 不同的metric，根据condition，有不同的统计策略
            metricAlertRuleMap(metricName).foreach(rule => {
              rule
            })
            metricRegistry.counter(metricName).inc()
            msg.reply(null)
          }
        }
      })
  }

  private def initRules(startPromise: Promise[Void]): Unit = {
    val ruleStr = config().getString(ALERT_RULE_CONFIG_KEY)
    if (StringUtils.isBlank(ruleStr)) {
      startPromise.fail("告警规则不能为空!配置key:" + ALERT_RULE_CONFIG_KEY)
    }
    metricAlertRuleMap = AlertRule.fromListJson(ruleStr).groupBy(_.metric)
  }

  override def stop(): Unit = {
    super.stop()
  }
}