package io.github.leibnizhu.vmonitor

import com.hazelcast.config.Config
import io.github.leibnizhu.vmonitor.Constants.EVENTBUS_MONITOR_JSON_PARAM_METRIC_NAME
import io.github.leibnizhu.vmonitor.util.FutureUtil
import io.vertx.core.json.JsonObject
import io.vertx.core.{Promise, Vertx}

import scala.concurrent.Await
import scala.concurrent.duration.Duration


/**
 * @author Leibniz on 2020/12/23 5:25 PM
 */
trait VMonitor {
  def start(): Unit = waitByPromise(startAsync)

  def startAsync(startPromise: Promise[Void]): Unit

  def stop(): Unit = waitByPromise(stopAsync)

  def stopAsync(stopPromise: Promise[Void]): Unit

  private def waitByPromise(fun: Function[Promise[Void], Unit]): Unit = {
    val promise = Promise.promise[Void]()
    fun(promise)
    Await.result(FutureUtil.vertxFutureToScala(promise.future()), Duration.Inf)
  }

  def collect(metricName: String, message: JsonObject): Unit
}

object VMonitor {
  def embedClusterVertx(address: String, env: String, ruleStr: String): VMonitor =
    new VMonitorEndpoint(address, env, ruleStr)

  //适配java
  def embedClusterVertx(address: String, env: String, ruleStr: String, clusterConfig: Config): VMonitor =
    new VMonitorEndpoint(address, env, ruleStr, clusterConfig)

  def specifyVertx(address: String, env: String, ruleStr: String, vertx: Vertx): VMonitor =
    new VMonitorEndpoint(address, env, ruleStr, vertx = vertx)


  def collect(address: String, metricName: String, message: JsonObject, vertx: Vertx): Unit = {
    if (vertx == null) {
      throw new IllegalStateException("vertx is not initialized!!!")
    }
    message.put(EVENTBUS_MONITOR_JSON_PARAM_METRIC_NAME, metricName)
    vertx.eventBus().publish(address, message) //这里要publish给所有节点
  }
}