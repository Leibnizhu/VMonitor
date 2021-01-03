package io.github.leibnizhu.vmonitor

import com.codahale.metrics.SharedMetricRegistries
import io.github.leibnizhu.vmonitor.Constants.{EVENTBUS_MONITOR_JSON_PARAM_METRIC_NAME, LISTEN_ADDRESS_CONFIG_KEY, MAIN_METRIC_NAME}
import io.vertx.core.AbstractVerticle
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

/**
 * @author Leibniz on 2020/12/30 2:15 PM
 */
class EventCollectorVerticle extends AbstractVerticle {
  private val log = LoggerFactory.getLogger(getClass)
  private val metricRegistry = SharedMetricRegistries.getOrCreate(MAIN_METRIC_NAME)

  override def start(): Unit = {
    log.info("======>启动:{},配置:{}", Array(getClass.getName, config()): _*)
    val address = config().getString(LISTEN_ADDRESS_CONFIG_KEY)
    vertx.eventBus().consumer(address)
      .handler((msg: Message[JsonObject]) => {
        log.debug("{}地址接收到eventbus消息:{}", Array(address, msg.body()): _*)
        val jsonBody = msg.body()
        if (jsonBody == null || jsonBody.isEmpty) {
          msg.reply(new JsonObject().put("errMsg", "监控记录请求内容为空"))
        } else {
          val metricName = jsonBody.getString(EVENTBUS_MONITOR_JSON_PARAM_METRIC_NAME)
          if (metricName == null || metricName.isEmpty) {
            msg.reply(new JsonObject().put("errMsg", "监听请求的指标名为空"))
          } else {
            metricRegistry.counter(metricName).inc()
            msg.reply(null)
          }
        }
      })
  }


  override def stop(): Unit = {
    super.stop()
  }
}