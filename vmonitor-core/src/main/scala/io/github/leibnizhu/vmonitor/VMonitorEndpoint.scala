package io.github.leibnizhu.vmonitor

import com.hazelcast.config.Config
import io.github.leibnizhu.vmonitor.Constants._
import io.github.leibnizhu.vmonitor.util.FutureUtil._
import io.github.leibnizhu.vmonitor.util.SystemUtil
import io.github.leibnizhu.vmonitor.wecom.SendWecomBotVerticle
import io.github.leibnizhu.vmonitor.wecom.message.MarkdownMessage
import io.github.leibnizhu.vmonitor.wecom.message.MarkdownMessage.MarkdownBuilder
import io.vertx.core._
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit


/**
 * @author Leibniz on 2020/12/23 5:10 PM
 */
class VMonitorEndpoint(address: String, env: String = "default", ruleStr: String,
                       hazelcastConfig: Config = null, var vertx: Vertx = null) extends VMonitor {
  private val log = LoggerFactory.getLogger(getClass)
  private var endpoint: String = _
  private val rules = if (StringUtils.isBlank(ruleStr)) throw new IllegalArgumentException("告警规则不能为空!")
  else AlertRule.fromListJson(ruleStr).map(_.validate())

  override def startAsync(startPromise: Promise[Void]): Unit = {
    if (vertx == null) {
      val mgr = if (hazelcastConfig == null) new HazelcastClusterManager(new Config()) else new HazelcastClusterManager(hazelcastConfig) //创建ClusterManger对象
      val options = new VertxOptions().setClusterManager(mgr)
        .setBlockedThreadCheckInterval(1).setBlockedThreadCheckIntervalUnit(TimeUnit.MINUTES) //设置到Vertx启动参数中
      Vertx.clusteredVertx(options, (vertxAr: AsyncResult[Vertx]) => {
        if (vertxAr.succeeded()) {
          val vertx = vertxAr.result()
          this.vertx = vertx
          log.info("启动集群模式的Vertx成功,deploymentIDs:{}", vertx.deploymentIDs())
          initVMonitor(vertx, startPromise, mgr)
        } else {
          val exp = vertxAr.cause()
          log.error("启动集群模式的Vertx失败:" + exp.getMessage, exp)
          startPromise.fail(exp)
        }
      })
    } else {
      initVMonitor(vertx, startPromise)
    }
  }

  private def initVMonitor(vertx: Vertx, startPromise: Promise[Void], mgr: HazelcastClusterManager = null) = {
    val deployConfig = new JsonObject().put(LISTEN_ADDRESS_CONFIG_KEY, address).put(ENVIRONMENT_CONFIG_KEY, env).put(ALERT_RULE_CONFIG_KEY, ruleStr)
    val deployOption = new DeploymentOptions().setConfig(deployConfig)
    val futures = List.fill(3)(Promise.promise[String]())
    vertx.deployVerticle(classOf[EventCollectorVerticle], deployOption, futures.head)
    vertx.deployVerticle(classOf[MetricsVerticle], deployOption, futures(1))
    vertx.deployVerticle(classOf[SendWecomBotVerticle], deployOption, futures(2))
    CompositeFuture
      .all(futures.head.future(), futures(1).future(), futures(2).future())
      .onSuccess((f: CompositeFuture) => {
        log.info("VMonitorEndpoint部署Verticle成功")
        this.endpoint = if (mgr == null) SystemUtil.hostName() else {
          val localAddr = mgr.getHazelcastInstance.getCluster.getLocalMember.getAddress
          s"${SystemUtil.hostName()}@${localAddr.getHost}:${localAddr.getPort}"
        }
        notifyAndRegisterHook(rules)
        startPromise.complete()
      })
      .onFailure((exp: Throwable) => {
        log.error("VMonitorEndpoint部署Verticle失败:" + exp.getMessage, exp)
        startPromise.fail(exp)
      })
  }

  def notifyAndRegisterHook(rules: Array[AlertRule]): Unit = {
    val wecomBotTokens = AlertRule.allWecomBotToken(rules)
    if (wecomBotTokens.nonEmpty) {
      val startMarkdownStr = makeMarkdownMessage(endpoint, "info", "启动")
      wecomBotTokens.foreach(token => {
        val wecomMsgJson = MarkdownMessage(token, startMarkdownStr).serializeToJsonObject()
        vertx.eventBus().request(SEND_WECOM_BOT_EVENTBUS_ADDR, wecomMsgJson, (_: AsyncResult[Message[JsonObject]]) => {})
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
        vertx.eventBus().request[JsonObject](SEND_WECOM_BOT_EVENTBUS_ADDR, wecomMsgJson,
          (_: AsyncResult[Message[JsonObject]]) => vertx.close(stopPromise))
      })
    } else {
      vertx.close(stopPromise)
    }
  }

  override def collect(metricName: String, message: JsonObject): Unit = {
    VMonitor.collect(address, metricName, message, vertx)
  }

  override def collect(metricName: String, message: String): Unit = {
    collect(metricName, new JsonObject(message))
  }
}
