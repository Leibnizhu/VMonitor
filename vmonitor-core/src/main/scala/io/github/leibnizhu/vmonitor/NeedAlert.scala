package io.github.leibnizhu.vmonitor

/**
 * @author Leibniz on 2021/01/4 5:49 PM
 */
object NeedAlert extends Enumeration {
  type NeedAlert = Value
  val NoNeed, AlertBegin, AlertEnd = Value
}
