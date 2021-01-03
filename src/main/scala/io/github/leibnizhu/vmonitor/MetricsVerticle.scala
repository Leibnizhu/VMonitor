package io.github.leibnizhu.vmonitor

import com.codahale.metrics.SharedMetricRegistries
import io.github.leibnizhu.vmonitor.Constants.{ALERT_RULE_CONFIG_KEY, ENVIRONMENT_CONFIG_KEY, MAIN_METRIC_NAME}
import io.vertx.core.shareddata.SharedData
import io.vertx.core.{AbstractVerticle, Promise}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

/**
 * @author Leibniz on 2020/12/30 2:21 PM
 */
class MetricsVerticle extends AbstractVerticle {
  private val log = LoggerFactory.getLogger(getClass)
  private val metricRegistry = SharedMetricRegistries.getOrCreate(MAIN_METRIC_NAME)
  private var env: String = _
  private var sd: SharedData = _

  override def start(startPromise: Promise[Void]): Unit = {
    log.info("======>启动:{},配置:{}", Array(getClass.getName, config()): _*)
    this.sd = vertx.sharedData()
    this.env = config().getString(ENVIRONMENT_CONFIG_KEY)
    initRules(startPromise)
    startPromise.complete()
  }

  private def initRules(startPromise: Promise[Void]): Unit = {
    val ruleStr = config().getString(ALERT_RULE_CONFIG_KEY)
    if (StringUtils.isBlank(ruleStr)) {
      startPromise.fail("告警规则不能为空!配置key:" + ALERT_RULE_CONFIG_KEY)
    }
    val ruleList = AlertRule.fromListJson(ruleStr)
    ruleList.foreach(registerAlertRule)
  }

  private def registerAlertRule(rule: AlertRule) = {
    val periodicMs = rule.checkPeriodMs
    val fsm = new AlertCheckFsm(rule, sd).init()
    vertx.setPeriodic(periodicMs, _ => {
      val satisfiedAlertCond = rule.satisfiedAlertCondition(metricRegistry)
      fsm.check(satisfiedAlertCond).onSuccess(if (_) rule.doAlert(env, vertx))
    })
  }

  override def stop(): Unit = {
    super.stop()
  }
}
