# mongo es river

一个根据时间戳同步mongo数据到elasticsearch的工具

#参考配置
```
es:
  host: localhost
  port: 9200
  username: elastic
  password: changme
mongo:
  host: localhost
  port: 27017
  username:
  password: 
rivers: 
  - mongo:
      database: mofangdb
      collection: communityUserTopic
      limit: 1000
    es:
      index: test-search-community-topic
    time_field:     # 代表时间字段，用于增量更新, 第一个时间用于作用为@timestamp时间，同事会用于增量更新，(由于多个时间的偏差，可能会造成bug，所以不建议使用多个时间字段)
#      - createAt
      - updateAt
    all_field_convert: false
    # k,v 结构。字段装换，可以定义字段名的转换，将mongo中字段为k的字段存入es时候时候字段v，v为空使用原名
    # 当all_field_convert为false时只转换当中存在的字段，没有的字段不存入es
    field_convert:
      topic:
      content:
      userId:
      module:
      delete:
    # 多久执行一次同步
    interval:
      minute: 1
    # 可以再同步过程中往es插入指定值得字段
    append_field:
#  - mongo:
#      database: mofangdb
#      collection: comment
#      limit: 1000
#      为同步到es的mongo数据添加查询条件
#      query:
#        "baseItem.module":
#          $in: ["community"]
#    es:
#      index: test-search-community-comment
#    time_field:
#      - date
#    all_field_convert: false
#    field_convert:
#      baseItem:
#      content:
#      userId:
#    interval:
#      minute: 1
```