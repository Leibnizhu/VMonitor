package io.github.leibnizhu.vmonitor

import com.hazelcast.config.Config
import io.github.leibnizhu.vmonitor.Constants.{ALERT_RULE_CONFIG_KEY, ENVIRONMENT_CONFIG_KEY, LISTEN_ADDRESS_CONFIG_KEY}
import io.github.leibnizhu.vmonitor.wecom.SendWecomBotVerticle
import io.vertx.core._
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit


/**
 * @author Leibniz on 2020/12/23 5:10 PM
 */
class VMonitorEndpoint(address: String, env: String = "default", ruleStr: String, hazelcastConfig: Config = null) extends VMonitor {
  private val log = LoggerFactory.getLogger(getClass)
  private var vertx: Vertx = _

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


  override def stopAsync(stopPromise: Promise[Void]): Unit = {
    vertx.close(stopPromise)
  }

  override def collect(message: JsonObject): Unit = {
    if (vertx == null) {
      throw new IllegalStateException("vertx is not initialized!!!")
    }
    vertx.eventBus().request(address, message)
      .onSuccess((msg: Message[JsonObject]) => {
        if (msg.body() != null)
          log.info("Eventbus返回:{}", msg.body())
      })
      .onFailure((e: Throwable) => {
        log.error("Eventbus返回:异常", e)
      })
  }
}
