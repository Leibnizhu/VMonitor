package io.github.leibnizhu.vmonitor.util

/**
 * @author Leibniz on 2020/12/31 5:16 PM
 */
object FutureUtil {
  type ScalaFuture[T] = scala.concurrent.Future[T]
  type VertxFuture[T] = io.vertx.core.Future[T]

  def vertxFutureToScala[T](vertxFuture: VertxFuture[T]): ScalaFuture[T] = {
    val promise = scala.concurrent.Promise[T]()
    vertxFuture.onSuccess(t => promise.success(t)).onFailure(exp => promise.failure(exp))
    promise.future
  }
}
