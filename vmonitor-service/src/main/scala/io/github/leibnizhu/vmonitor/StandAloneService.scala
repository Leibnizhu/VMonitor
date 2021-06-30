package io.github.leibnizhu.vmonitor


import io.github.leibnizhu.vmonitor.util.FutureUtil._
import io.vertx.core.buffer.Buffer
import io.vertx.core.cli.{CLI, Option}
import io.vertx.core.http.{HttpServer, HttpServerRequest}
import io.vertx.core.json.JsonObject
import io.vertx.core.{AsyncResult, Vertx}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * @author Leibniz on 2021/01/11 11:58 AM
 */
object StandAloneService {
  private val log = LoggerFactory.getLogger(classOf[StandAloneService])

  def main(args: Array[String]): Unit = {
    System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory")
    System.setProperty("vertx.disableFileCaching", "true")
    val cli = CLI.create("copy")
      .setSummary("A command line interface to copy files.")
      .addOption(new Option().setLongName("environment").setShortName("e").setDescription("Deploy environment name").setRequired(true))
      .addOption(new Option().setLongName("address").setShortName("a").setDescription("EventBus address").setRequired(false).setDefaultValue("VMonitorEB"))
      .addOption(new Option().setLongName("rule").setShortName("r").setDescription("Alert rules json file").setRequired(false).setDefaultValue("rule.json"))
      .addOption(new Option().setLongName("port").setShortName("p").setDescription("Http listen port, if not supply, will not start http server").setRequired(false))
    val commandLine = cli.parse(args.toList.asJava)
    val vertx = Vertx.vertx()
    val ruleStr = vertx.fileSystem().readFileBlocking(commandLine.getOptionValue("rule")).toString()
    val address = commandLine.getOptionValue[String]("address")
    val vMonitor = VMonitor.specifyVertx(
      address,
      commandLine.getOptionValue("environment"),
      ruleStr, vertx)
    vMonitor.start()
    val portStr = commandLine.getOptionValue[String]("port")
    if (StringUtils.isNotBlank(portStr)) {
      vertx.createHttpServer()
        .requestHandler((req: HttpServerRequest) => {
          val resp = req.response()
          val uri = req.uri()
          val metric = uri.substring(uri.lastIndexOf("/") + 1)
          req.bodyHandler((buf: Buffer) => {
            if (buf.length() > 0) {
              VMonitor.collect(address, metric, new JsonObject(buf), vertx)
              resp.end("success")
            } else {
              resp.end("empty request body")
            }
          })
          //            .onSuccess((buf: Buffer) => {
          //            })
          //            .onFailure((e: Throwable) => {
          //              log.error(e.getMessage, e)
          //              resp.end("failed:" + e.getMessage)
          //            })
        })
        .listen(portStr.toInt, (listenAr: AsyncResult[HttpServer]) => {
          if (listenAr.succeeded()) {
            log.info("监听{}端口的HTTP服务器启动成功", portStr)
          } else {
            log.error("监听{}端口的HTTP服务器失败，原因：{}", Array(portStr, listenAr.cause().getLocalizedMessage): _*)
          }
        })
    }
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        vMonitor.stop()
      }
    }))
  }
}

class StandAloneService //java的main方法要用
