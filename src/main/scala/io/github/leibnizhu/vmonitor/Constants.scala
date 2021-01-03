package io.github.leibnizhu.vmonitor

object Constants {
  //Eventbus地址
  val SEND_WECOM_BOT_EVENTBUS_ADDR = "sendWecomBotMessage"

  //EventBus传输企业微信bot发送请求的json的key和部分固定value
  val EVENTBUS_WECOM_BOT_JSON_PARAM_TYPE = "type"
  val EVENTBUS_WECOM_BOT_JSON_PARAM_TYPE_MARKDOWN = "markdown"
  val EVENTBUS_WECOM_BOT_JSON_PARAM_TYPE_TEXT = "text"
  val EVENTBUS_WECOM_BOT_JSON_PARAM_TOKEN = "token"
  val EVENTBUS_WECOM_BOT_JSON_PARAM_CONTENT = "content"
  val EVENTBUS_WECOM_BOT_JSON_PARAM_MENTION_LIST = "mentionMobileList"

  //EventBus传输监控事件的key
  val EVENTBUS_MONITOR_JSON_PARAM_METRIC_NAME = "metricName"

  //HttpVerticle请求参数
  val REQ_PARAM_WECOM_BOT_TOKEN: String = "wecomBotToken"
  val REQ_PARAM_WECOM_BOT_TYPE: String = "wecomBotType"
  val REQ_PARAM_MENTIONED_LIST: String = "mentionedList" //Text是手机号，Markdown是邮箱
  //企业微信机器人地址
  val WECOM_BOT_API_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key="

  val LISTEN_ADDRESS_CONFIG_KEY = "listenAddress"
  val ALERT_RULE_CONFIG_KEY = "alertRule"
  val ENVIRONMENT_CONFIG_KEY = "environment"

  val MAIN_METRIC_NAME = "main"
}
