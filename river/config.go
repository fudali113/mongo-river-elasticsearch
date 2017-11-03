/*
	river
*/
package river

import "fmt"

type Config struct {
	Es     Es
	Mongo  Mongo
	Rivers []RiverInfo
}

type Es struct {
	Host     string
	Port     string
	Username string
	Password string
}

type IndexInfo struct {
	Type  string
	Index string
}

type Mongo struct {
	Host     string
	Port     string
	Username string
	Password string
}

// GetUrl 获取mongo连接字符串
func (mongo Mongo) GetUrl() string {
	if mongo.Username != "" {
		return fmt.Sprintf("mongodb://%s:%s@%s:%s", mongo.Username, mongo.Password, mongo.Host, mongo.Port)
	}
	return fmt.Sprintf("mongodb://%s:%s", mongo.Host, mongo.Port)
}

type CollectionInfo struct {
	Database   string
	Collection string
	Limit      int
	Query		map[string]interface{}
}

// Time 语义相关的时间
// such as ["day:1","hour":2,"second": 30] = 1天2小时30秒
type Time map[string]int32

// GetSecond 根据时间信息获取秒数
func (t Time) GetSecond() int32 {
	var second int32
	for k, v := range t {
		switch k {
		case "year":
			second += 60 * 60 * 24 * 365 * v
		case "month":
			second += 60 * 60 * 24 * 30 * v
		case "day":
			second += 60 * 60 * 24 * v
		case "hour":
			second += 60 * 60 * v
		case "minute":
			second += 60 * v
		case "second":
			second += v
		}
	}
	return second
}

type RiverInfo struct {
	Name  string
	Mongo CollectionInfo
	Es    IndexInfo
	// 间热多久更新一次
	Interval Time
	// 能代表时间的字段，用户根据实际增量更新
	TimeField []string `yaml:"time_field"`
	// 是否全部字段都装换
	AllFieldConvert bool `yaml:"all_field_convert""`
	// 字段名字转换。 AllFieldConvert为true时没包含的使用原名，为false时只转换包含的字段
	FieldConvert map[string]string `yaml:"field_convert"`
	AppendField  map[string]string `yaml:"append_field"`
}

func (river RiverInfo) GetName() string {
	if river.Name == "" {
		return river.Mongo.Collection + "-to-" + river.Es.Index
	}
	return river.Name
}
