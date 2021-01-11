## 实例JSON
```json
[
  {
    "name": "SchedulerError监控",
    "metric": {
      /*指标名，需要于代码里面写入指标的一样，建议格式类似x.y.z*/
      "name": "serv-etl.SchedulerError",
      "filter": [],
      "groupField": [],
      "groupAggFunc": null,
      "groupAggField": "key",
      "sampleInterval": "1m",
      "sampleAggFunc": "uniqueCount",
      "sampleAggField": "cost"
    },
    "period": {
      "every": "10s",
      /*每隔多久检查一次*/
      "pend": "10s"
      /*出现问题后进入pending状态持续多久才发出警报*/
    },
    "condition": {
      /*统计最近多久的时间*/
      "last": "30s",
      /*支持 max,min,avg,median,stdDev,count,sum等*/
      "method": "avg",
      /*比较符， 支持 >,>=,<,<=,=等等*/
      "op": ">=",
      /*比较的目标值*/
      "threshold": 3
    },
    "alert": {
      /*目前只支持WecomBot企业微信bot，和Log只打日志*/
      "method": "WecomBot",
      "times": 3,
      "interval": "90s",
      "config": {
        "token": "xxxxxxxxx",
        /*自定义的消息*/
        "message": "SchedulerError最近90s出现3次以上"
      }
    }
  }
]
```