package io.github.leibnizhu.vmonitor.util

import io.vertx.core.Handler

/**
 * @author Leibniz on 2020/12/31 5:16 PM
 */
object FutureUtil {
  type ScalaFuture[T] = scala.concurrent.Future[T]
  type VertxFuture[T] = io.vertx.core.Future[T]

  def vertxFutureToScala[T](vertxFuture: VertxFuture[T]): ScalaFuture[T] = {
    val promise = scala.concurrent.Promise[T]()
    vertxFuture.onSuccess((t: T) => promise.success(t)).onFailure((exp: Throwable) => promise.failure(exp))
    promise.future
  }


  implicit def toJavaFunction[U, V](f: U => V): java.util.function.Function[U, V] = new java.util.function.Function[U, V] {
    override def apply(t: U): V = f(t)
  }

  //  implicit def toHandler[V](f: V => Unit): Handler[V] = new Handler[V] {
  //    override def handle(event: V): Unit = f(event)
  //  }

  implicit def toHandler[U, V](f: U => V): Handler[U] = new Handler[U] {
    override def handle(event: U): Unit = f(event)
  }

  implicit def toSupplier[V](f: () => V): java.util.function.Supplier[V] = new java.util.function.Supplier[V] {
    override def get(): V = f()
  }

  implicit def toSupplier[U, V](f: U => V): java.util.function.Consumer[U] = new java.util.function.Consumer[U] {
    override def accept(t: U): Unit = f(t)
  }

}
