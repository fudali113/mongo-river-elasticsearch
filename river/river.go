package river

import (
	"context"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"time"
	"encoding/json"
)

// InitConfig 初始化配置
func InitConfig(configDir string) (config *Config, err error) {
	bytes, err := ioutil.ReadFile(configDir)
	if err != nil {
		return
	}
	config = &Config{
		Es: Es{
			Host:     "localhost",
			Port:     "9200",
			Username: "elastic",
			Password: "changeme",
		},
		Mongo: Mongo{
			Host:     "localhost",
			Port:     "27017",
			Username: "root",
			Password: "root",
		},
		Rivers: []RiverInfo{},
	}
	err = yaml.Unmarshal(bytes, config)
	return
}

var RuleContext = map[string]context.CancelFunc{}

// Run 根据配置文件运行
func Run(config Config, exit chan string) error {
	// TODO
	rivers := []*River{}
	for _, river := range config.Rivers {
		session, err := mgo.Dial(config.Mongo.GetUrl())
		if err != nil {
			return err
		}
		var tick *time.Ticker
		if len(river.Interval) != 0 {
			tick = time.NewTicker(time.Duration(river.Interval.GetSecond()) * time.Second)
		}
		rivers = append(rivers, &River{
			collection: session.DB(river.Mongo.Database).C(river.Mongo.Collection),
			limit:      river.Mongo.Limit,
			index: Index{
				Es:        config.Es,
				IndexInfo: river.Es,
			},
			tick:      tick,
			RiverInfo: river,
		})
	}
	for _, river := range rivers {
		ctx, cancelFunc := context.WithCancel(context.Background())
		river.run(ctx)
		RuleContext[river.Name] = cancelFunc
	}
	return nil
}

type River struct {
	RiverInfo
	collection *mgo.Collection
	limit      int
	index      Index
	tick       *time.Ticker
}

func (river *River) run(ctx context.Context) {
	runFunc := func() {
		time, err := river.index.LastDocTime()
		if err != nil {
			log.Println("获取最后时间出错: ", err)
			return
		}
		var query *mgo.Query
		criteria := river.Mongo.Query
		if criteria == nil {
			criteria = bson.M{}
		}
		cleanupMapValue(criteria)
		// 时间字段与time都不为空时添加查询条件
		if time != nil && len(river.TimeField) > 0 {
			timeQuery := []bson.M{}
			for _, v := range river.TimeField {
				// FIXME
				// "gte"能保证数据都能获取到，不然出现相同时间的可能会出现有些数据无法转移的情况
				// 但是这样子可能出现有一批数据会重复更新
				timeQuery = append(timeQuery, bson.M{v: bson.M{"$gte": time}})
			}
			criteria["$or"] = timeQuery
		}
		jsonQueryByte, _ := json.Marshal(criteria)
		log.Println("mongo query string: ", string(jsonQueryByte))
		query = river.collection.Find(criteria).Sort(river.TimeField...)
		// 查询
		if river.limit > 0 {
			query.Limit(river.limit)
		}
		result := []bson.M{}
		err = query.All(&result)
		if err != nil {
			log.Println("查询mongodb出错: ", err)
			return
		}
		if time != nil && len(result) <= 1 {
			log.Println("有更新时间但是只有一条更新记录，此条记录不用更新，time: ")
			return
		}
		// 字段映射
		if !river.AllFieldConvert && len(river.FieldConvert) != 0 {
			filterResult := []bson.M{}
			for _, one := range result {
				filter := bson.M{}
				// 如果有默认添加字段，首先添加字段到容器中
				if len(river.AppendField) > 0 {
					for k, v := range river.AppendField {
						filter[k] = v
					}
				}
				for k, v := range one {
					if _, ok := river.FieldConvert[k]; ok {
						newK := river.FieldConvert[k]
						if newK != "" {
							filter[newK] = v
						} else {
							filter[k] = v
						}
					}
				}
				if _, ok := one["_id"]; ok {
					filter["_id"] = one["_id"]
				}
				// FIXME 由于多个时间的偏差，可能会造成bug，所以不建议使用多个时间字段
				for _, v := range river.TimeField {
					if _, ok := one[v]; ok {
						filter["@timestamp"] = one[v]
						break
					}
				}
				filterResult = append(filterResult, filter)
			}
			result = filterResult
		}
		if len(result) == 0 {
			log.Println("插入数组数据为空, 此次没有同步任何数据")
			return
		}
		// 插入es
		info := river.index.PutAll(result)
		log.Println(info.Msg)
		if info.Err != nil {
			log.Println("数据插入es出错: ", err)
			return
		}
	}
	// 启动时运行一次，如果没有间隔时间，只执行一次
	runFunc()
	if river.tick == nil {
		return
	}
	go func() {
		for {
			select {
			case <-river.tick.C:
				runFunc()
			case <-ctx.Done():
				break
			}
		}
	}()
}
