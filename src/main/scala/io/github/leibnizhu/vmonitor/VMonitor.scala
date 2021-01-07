package io.github.leibnizhu.vmonitor

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

  def specificVertx(address: String, env: String, ruleStr: String, vertx: Vertx): VMonitor =
    new VMonitorEndpoint(address, env, ruleStr, vertx = vertx)
}