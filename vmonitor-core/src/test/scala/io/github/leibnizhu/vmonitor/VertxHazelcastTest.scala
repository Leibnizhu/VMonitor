package io.github.leibnizhu.vmonitor

import com.codahale.metrics.MetricRegistry
import com.hazelcast.config.Config
import io.github.leibnizhu.vmonitor.util.FutureUtil._
import io.vertx.core.eventbus.Message
import io.vertx.core.{AsyncResult, Vertx, VertxOptions}
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.concurrent.Promise

/**
 * @author Leibniz on 2020/12/23 10:28 AM
 */
class VertxHazelcastTest extends FunSuite {
  private val log = LoggerFactory.getLogger(getClass)

  test("HazelcastTest1") {
    val promise = Promise.apply[Object]()
    val hazelcastConfig = new Config
    val mgr = new HazelcastClusterManager(hazelcastConfig) //创建ClusterManger对象
    val options = new VertxOptions().setClusterManager(mgr) //设置到Vertx启动参数中
    Vertx.clusteredVertx(options, (vertxAr: AsyncResult[Vertx]) => {
      val vertx = vertxAr.result()
      val registry = new MetricRegistry()
      val counter = registry.counter(MetricRegistry.name(classOf[VertxHazelcastTest], "test-counter"))
      counter.inc(10)
      vertx.eventBus().consumer[String]("testAddress")
        .handler((msg: Message[String]) => {
          log.info("cluster:{}", mgr.getNodeID)
          log.info("接收到eventbus:{}", msg.body())
          msg.reply("收到啦")
          Thread.sleep(1000)
          promise.success(null)
        })
    })
    Thread.sleep(5000)
    new Thread(new Runnable {
      override def run(): Unit = {
        val promise = Promise.apply[Object]()
        val hazelcastConfig = new Config
        hazelcastConfig.getNetworkConfig.setPort(2222)
        val mgr = new HazelcastClusterManager(hazelcastConfig) //创建ClusterManger对象
        val options = new VertxOptions().setClusterManager(mgr) //设置到Vertx启动参数中
        val log = LoggerFactory.getLogger(classOf[Thread])
        Vertx.clusteredVertx(options, (vertxAr: AsyncResult[Vertx]) => {
          val vertx = vertxAr.result()
          log.info("cluster:{}", mgr.getNodeID)
          val registry = new MetricRegistry()
          val counter = registry.counter(MetricRegistry.name(classOf[VertxHazelcastTest], "test-counter"))
          log.info("counter :{}", counter.getCount)
          vertx.eventBus()
            .request[String]("testAddress", "hahahha", (reqAr: AsyncResult[Message[String]]) => {
              if (reqAr.succeeded()) {
                log.info("Eventbus返回:{}", reqAr.result().body())
                promise.success(null)
              } else {
                log.error("Eventbus返回:异常", reqAr.cause())
                promise.success(null)
              }
            })
        })
        while (!promise.future.isCompleted) {
          Thread.sleep(1000)
        }
      }
    }).start()
    while (!promise.future.isCompleted) {
      Thread.sleep(1000)
    }
  }

  //  test("HazelcastTest2")
}
