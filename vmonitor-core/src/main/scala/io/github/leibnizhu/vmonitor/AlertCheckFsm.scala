package io.github.leibnizhu.vmonitor

import io.github.leibnizhu.vmonitor.AlertStatus.{Alert, AlertPending, AlertStatus, Normal, NormalPending}
import io.github.leibnizhu.vmonitor.NeedAlert.{AlertBegin, AlertEnd, NeedAlert, NoNeed}
import io.github.leibnizhu.vmonitor.util.FutureUtil._
import io.vertx.core.shareddata.{AsyncMap, Lock, SharedData}
import io.vertx.core.{AsyncResult, Future, Promise}
import org.slf4j.LoggerFactory

/**
 * 简陋定制版fsm，检查pending和告警的逻辑
 *
 * @author Leibniz on 2021/01/3 12:51 AM
 */
class AlertCheckFsm(rule: AlertRule, sd: SharedData) {
  private val log = LoggerFactory.getLogger(getClass)
  private val statusMapKey = "status"
  private val startPendingTimeMapKey = "startPendingTime"
  private val alertedCountMapKey = "alertedCount"
  private val lastAlertTimeMapKey = "lastAlertTime"

  def init(): AlertCheckFsm = {
    sd.getAsyncMap[String, AnyRef](rule.alertFsmMapName)
      .onSuccess((asyncMap: AsyncMap[String, AnyRef]) => asyncMap.put(statusMapKey, Normal))
      .onFailure((e: Throwable) => log.error(s"获取${rule.alertFsmMapName}的异步Map失败:${e.getMessage}", e))
    this
  }

  def check(satisfiedCondition: Boolean): Future[NeedAlert] = {
    val needAlertPromise = Promise.promise[NeedAlert]()
    sd.getLocalLock(rule.alertFsmLockName)
      .onSuccess((lock: Lock) => {
        val processedPromise = Promise.promise[NeedAlert]()
        doCheck(satisfiedCondition, processedPromise)
        processedPromise.future().onComplete((ar: AsyncResult[NeedAlert]) => {
          needAlertPromise.complete(if (ar.succeeded()) ar.result() else NoNeed)
          lock.release()
        })
      })
      .onFailure((e: Throwable) => {
        log.error(s"获取${rule.alertFsmLockName}的分布式锁失败:${e.getMessage}", e)
        needAlertPromise.complete(NoNeed)
      })
    needAlertPromise.future()
  }

  private def doCheck(satisfiedCondition: Boolean, promise: Promise[NeedAlert]): Unit = {
    sd.getAsyncMap[String, AnyRef](rule.alertFsmMapName)
      .onSuccess((asyncMap: AsyncMap[String, AnyRef]) => asyncMap.get(statusMapKey)
        .onSuccess((s: AnyRef) => s match {
          case status: AlertStatus =>
            status match {
              case Normal => processNormal(satisfiedCondition, asyncMap, promise)
              case AlertPending => processAlertPending(satisfiedCondition, asyncMap, promise)
              case Alert => processAlert(satisfiedCondition, asyncMap, promise)
              case NormalPending => processNormalPending(satisfiedCondition, asyncMap, promise)
            }
          case statusObj =>
            log.error("不支持的状态类型:" + statusObj)
            promise.complete(NoNeed)
        })
        .onFailure((e: Throwable) => {
          log.error(s"获取${rule.alertFsmMapName}的异步Map的${statusMapKey}属性失败:${e.getMessage}", e)
          promise.fail(e)
        })
      )
      .onFailure((e: Throwable) => {
        log.error(s"获取${rule.alertFsmMapName}的异步Map失败:${e.getMessage}", e)
        promise.fail(e)
      })
  }

  private def processNormal(satisfiedCondition: Boolean, asyncMap: AsyncMap[String, AnyRef], promise: Promise[NeedAlert]) = {
    if (satisfiedCondition) {
      log.info("规则{}在Normal状态触发警报条件,进入AlertPending状态", rule.name)
      asyncMap.put(statusMapKey, AlertPending)
        .compose((_: Void) => asyncMap.put(startPendingTimeMapKey, Long.box(System.currentTimeMillis())))
        .onComplete((ar: AsyncResult[Void]) => defaultCompleter(ar, promise))
    } else {
      promise.complete(NoNeed)
    }
  }

  private def processAlertPending(satisfiedCondition: Boolean, asyncMap: AsyncMap[String, AnyRef], promise: Promise[NeedAlert]) = {
    if (satisfiedCondition) {
      asyncMap.get(startPendingTimeMapKey)
        .onSuccess((startPendingTime: AnyRef) => if (System.currentTimeMillis() - startPendingTime.asInstanceOf[Long] > rule.pendingMs) {
          log.info("规则{}在AlertPending状态持续保持告警条件{},进入Alert状态,发出第1次告警通知", Array(rule.name, rule.period.pend): _*)
          asyncMap.put(statusMapKey, Alert)
            .compose((_: Void) => asyncMap.put(alertedCountMapKey, Int.box(1)))
            .compose((_: Void) => asyncMap.put(lastAlertTimeMapKey, Long.box(System.currentTimeMillis())))
            .onComplete((ar: AsyncResult[Void]) => defaultCompleter(ar, promise, AlertBegin))
        } else {
          promise.complete(NoNeed)
        })
        .onFailure((e: Throwable) => {
          log.error(s"获取${rule.alertFsmMapName}的异步Map的${startPendingTimeMapKey}属性失败:${e.getMessage}", e)
          promise.fail(e)
        })
    } else {
      log.info("规则{}在AlertPending状态由于在{}时间内不满足告警条件,恢复到Normal状态", Array(rule.name, rule.period.pend): _*)
      asyncMap.put(statusMapKey, Normal)
        .onComplete((ar: AsyncResult[Void]) => defaultCompleter(ar, promise))
    }
  }

  private def processNormalPending(satisfiedCondition: Boolean, asyncMap: AsyncMap[String, AnyRef], promise: Promise[NeedAlert]) = {
    if (satisfiedCondition) {
      log.info("规则{}在NormalPending状态由于在{}时间内又满足告警条件,恢复到Alert状态", Array(rule.name, rule.period.pend): _*)
      asyncMap.put(statusMapKey, Alert)
        .onComplete((ar: AsyncResult[Void]) => defaultCompleter(ar, promise))
    } else {
      asyncMap.get(startPendingTimeMapKey)
        .onSuccess((startPendingTime: AnyRef) => if (System.currentTimeMillis() - startPendingTime.asInstanceOf[Long] > rule.pendingMs) {
          log.info("规则{}在NormalPending状态持续保持不满足告警条件{},进入Normal状态", Array(rule.name, rule.period.pend): _*)
          asyncMap.put(statusMapKey, Normal)
            .compose((_: Void) => asyncMap.put(alertedCountMapKey, Int.box(0)))
            .compose((_: Void) => asyncMap.put(lastAlertTimeMapKey, Long.box(0)))
            .onComplete((ar: AsyncResult[Void]) => defaultCompleter(ar, promise, AlertEnd))
        } else {
          promise.complete(NoNeed)
        })
        .onFailure((e: Throwable) => {
          log.error(s"获取${rule.alertFsmMapName}的异步Map的${startPendingTimeMapKey}属性失败:${e.getMessage}", e)
          promise.fail(e)
        })
    }
  }

  private def processAlert(satisfiedCondition: Boolean, asyncMap: AsyncMap[String, AnyRef], promise: Promise[NeedAlert]) = {
    if (satisfiedCondition) {
      asyncMap.get(alertedCountMapKey)
        .onSuccess((alertCnt: AnyRef) => {
          if (alertCnt.asInstanceOf[Int] < rule.alert.times) {
            asyncMap.get(lastAlertTimeMapKey)
              .onSuccess((lastAlertTime: AnyRef) => {
                val lastAlertInterval = System.currentTimeMillis() - lastAlertTime.asInstanceOf[Long]
                if (lastAlertInterval > rule.alertIntervalMs) {
                  log.info(s"规则'${rule.name}'在Alert状态已发出${alertCnt}次(<${rule.alert.times})告警通知," +
                    s"距离上次告警通知已${lastAlertInterval}ms(>${rule.alert.interval}),再次发出告警通知")
                  asyncMap.put(alertedCountMapKey, Int.box(alertCnt.asInstanceOf[Int] + 1))
                    .compose((_: Void) => asyncMap.put(lastAlertTimeMapKey, Long.box(System.currentTimeMillis())))
                    .onComplete((ar: AsyncResult[Void]) => defaultCompleter(ar, promise, AlertBegin))
                } else {
                  promise.complete(NoNeed)
                }
              })
              .onFailure((e: Throwable) => {
                log.error(s"获取${rule.alertFsmMapName}的异步Map的${lastAlertTimeMapKey}属性失败:${e.getMessage}", e)
                promise.fail(e)
              })
          } else {
            log.debug(s"规则'${rule.name}'在Alert状态已发出${alertCnt}次(>=${rule.alert.times})告警通知,不再通知")
            promise.complete(NoNeed)
          }
        })
        .onFailure((e: Throwable) => {
          log.error(s"获取${rule.alertFsmMapName}的异步Map的${alertedCountMapKey}属性失败:${e.getMessage}", e)
          promise.fail(e)
        })
    } else {
      log.info("规则{}在Alert状态由于不满足告警条件,恢复到NormalPending状态", rule.name)
      asyncMap.put(statusMapKey, NormalPending)
        .compose((_: Void) => asyncMap.put(startPendingTimeMapKey, Long.box(System.currentTimeMillis())))
        .onComplete((ar: AsyncResult[Void]) => defaultCompleter(ar, promise))
    }
  }


  private def defaultCompleter[T](ar: AsyncResult[T], promise: Promise[NeedAlert], result: NeedAlert = NoNeed): Unit = {
    if (ar.failed()) {
      log.error(ar.cause().getMessage, ar.cause())
    }
    promise.complete(result)
  }
}

private object AlertStatus extends Enumeration {
  type AlertStatus = Value
  val Normal, AlertPending, NormalPending, Alert = Value
}
