package io.github.leibnizhu.vmonitor

import io.github.leibnizhu.vmonitor.util.FutureUtil
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject

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

  def collect(message: JsonObject): Unit
}
