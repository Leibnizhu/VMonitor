package io.github.leibnizhu.vmonitor

import com.hazelcast.config.Config
import io.github.leibnizhu.vmonitor.Constants._
import io.github.leibnizhu.vmonitor.util.SystemUtil
import io.github.leibnizhu.vmonitor.wecom.SendWecomBotVerticle
import io.github.leibnizhu.vmonitor.wecom.message.MarkdownMessage
import io.github.leibnizhu.vmonitor.wecom.message.MarkdownMessage.MarkdownBuilder
import io.vertx.core._
import io.vertx.core.json.JsonObject
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit


/**
 * @author Leibniz on 2020/12/23 5:10 PM
 */
class VMonitorEndpoint(address: String, env: String = "default", ruleStr: String, hazelcastConfig: Config = null) extends VMonitor {
  private val log = LoggerFactory.getLogger(getClass)
  private var vertx: Vertx = _
  if (StringUtils.isBlank(ruleStr)) {
    throw new IllegalArgumentException("告警规则不能为空!")
  }
  private val rules = AlertRule.fromListJson(ruleStr).map(_.validate())
  private var endpoint: String = _

  override def startAsync(startPromise: Promise[Void]): Unit = {
    val mgr = if (hazelcastConfig == null) new HazelcastClusterManager(new Config()) else new HazelcastClusterManager(hazelcastConfig) //创建ClusterManger对象
    val options = new VertxOptions().setClusterManager(mgr)
      .setBlockedThreadCheckInterval(1).setBlockedThreadCheckIntervalUnit(TimeUnit.MINUTES) //设置到Vertx启动参数中
    Vertx.clusteredVertx(options)
      .onSuccess(vertx => {
        this.vertx = vertx
        log.info("启动集群模式的Vertx成功,deploymentIDs:{}", vertx.deploymentIDs())
        val deployConfig = new JsonObject().put(LISTEN_ADDRESS_CONFIG_KEY, address).put(ENVIRONMENT_CONFIG_KEY, env).put(ALERT_RULE_CONFIG_KEY, ruleStr)
        val deployOption = new DeploymentOptions().setConfig(deployConfig)
        CompositeFuture
          .all(vertx.deployVerticle(classOf[EventCollectorVerticle], deployOption),
            vertx.deployVerticle(classOf[MetricsVerticle], deployOption),
            vertx.deployVerticle(classOf[SendWecomBotVerticle], deployOption))
          .onSuccess(_ => {
            log.info("VMonitorEndpoint部署Verticle成功")
            this.endpoint = s"${SystemUtil.hostName()}@${mgr.getNodeInfo.host()}:${mgr.getNodeInfo.port()}"
            notifyAndRegisterHook(rules, mgr)
            startPromise.complete()
          })
          .onFailure(exp => {
            log.error("VMonitorEndpoint部署Verticle失败:" + exp.getMessage, exp)
            startPromise.fail(exp)
          })
      })
      .onFailure(exp => {
        log.error("启动集群模式的Vertx失败:" + exp.getMessage, exp)
        startPromise.fail(exp)
      })
  }

  def notifyAndRegisterHook(rules: Array[AlertRule], mgr: HazelcastClusterManager): Unit = {
    val wecomBotTokens = AlertRule.allWecomBotToken(rules)
    if (wecomBotTokens.nonEmpty) {
      val startMarkdownStr = makeMarkdownMessage(endpoint, "info", "启动")
      wecomBotTokens.foreach(token => {
        val wecomMsgJson = MarkdownMessage(token, startMarkdownStr).serializeToJsonObject()
        vertx.eventBus().request(SEND_WECOM_BOT_EVENTBUS_ADDR, wecomMsgJson)
      })
    }
  }


  private def makeMarkdownMessage(endpoint: String, color: String, status: String) = {
    new MarkdownBuilder()
      .colored(color, "VMonitor节点" + status).newLine()
      .quoted().text("环境: ").text(env).newLine()
      .quoted().text("时刻: ").text(SystemUtil.currentTime()).newLine()
      .quoted().text("节点: ").text(endpoint).newLine()
      .quoted().text("进程ID: ").text(SystemUtil.currentPid().toString).newLine()
      .toMarkdownString
  }

  override def stopAsync(stopPromise: Promise[Void]): Unit = {
    val wecomBotTokens = AlertRule.allWecomBotToken(rules)
    if (wecomBotTokens.nonEmpty) {
      val stopMarkdownStr = makeMarkdownMessage(endpoint, "warning", "关闭")
      wecomBotTokens.foreach(token => {
        val wecomMsgJson = MarkdownMessage(token, stopMarkdownStr).serializeToJsonObject()
        vertx.eventBus().request(SEND_WECOM_BOT_EVENTBUS_ADDR, wecomMsgJson)
      })
    }
    vertx.close(stopPromise)
  }

  override def collect(metricName: String, message: JsonObject): Unit = {
    if (vertx == null) {
      throw new IllegalStateException("vertx is not initialized!!!")
    }
    message.put(EVENTBUS_MONITOR_JSON_PARAM_METRIC_NAME, metricName)
    vertx.eventBus().publish(address, message) //这里要publish给所有节点
    //      .onSuccess((msg: Message[JsonObject]) => {
    //        if (msg.body() != null)
    //          log.info("Eventbus返回:{}", msg.body())
    //      })
    //      .onFailure((e: Throwable) => {
    //        log.error("Eventbus返回:异常", e)
    //      })
  }
}
