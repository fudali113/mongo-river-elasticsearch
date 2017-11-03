package river

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

// Index 代表一个index，当type不为空时代表一个index下的type
type Index struct {
	Es
	IndexInfo
}

func (index Index) GetUrl() string {
	addr := ""
	if strings.HasPrefix(index.Host, "http://") || strings.HasPrefix(index.Host, "https://") {
		addr += index.Host
	} else {
		addr += "http://" + index.Host
	}
	addr += ":" + index.Port
	return addr + "/" + index.Index + "/" + index.GetType()
}

func (index Index) GetType() string {
	if index.Type == "" {
		return "doc"
	}
	return index.Type
}

type EsResponseInfo struct {
	Msg string
	Err error
}

// PutAll 以数组方式插入
// see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
func (index Index) PutAll(docs []bson.M) EsResponseInfo {
	body := []byte{}
	for _, doc := range docs {
		if _, ok := doc["_id"]; ok {
			actionAndMetaData := bson.M{"update": bson.M{"_id": doc["_id"]}}
			bytes, _ := json.Marshal(actionAndMetaData)
			body = add(body, bytes)
			delete(doc, "_id")
			optionalSource := bson.M{"doc": doc, "doc_as_upsert": true}
			bytes, err := json.Marshal(optionalSource)
			if err != nil {
				log.Println("解析单条doc数据出错，", err)
				continue
			}
			body = add(body, bytes)
		}
	}
	url := index.GetUrl() + "/" + "_bulk"
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(body))
	if err != nil {
		return EsResponseInfo{Err:err}
	}
	req.SetBasicAuth(index.Username, index.Password)
	req.Header.Set("Content-Type", "application/x-ndjson")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return EsResponseInfo{Err:err}
	}
	if response.StatusCode > 210 {
		return EsResponseInfo{Err:fmt.Errorf("es插入请求出错，返回状态码：%s", response.StatusCode)}
	}
	resBody, _ := ioutil.ReadAll(response.Body)
	return EsResponseInfo{Msg:"同步成功，同步数据 " + fmt.Sprintf("%d", len(docs))  + " 条 \n  " +
		"es response body: " + string(resBody) + "\r\n"}
}

func add(origin []byte, add []byte) []byte {
	return append(append(origin, add...), []byte("\n")...)
}

// Put 单个插入
func (index Index) Put(doc bson.M) error {
	// TODO
	return nil
}

type query = map[string]interface{}

// LastDocTime 获取当前index最末尾文档时间
func (index Index) LastDocTime() (t *time.Time, err error) {
	body, _ := json.Marshal(query{"sort": []query{{"@timestamp": "desc"}}, "query": query{}, "size": 1})
	req, err := http.NewRequest("POST", index.GetUrl()+"/_search", bytes.NewBuffer(body))
	if err != nil {
		return
	}
	req.SetBasicAuth(index.Username, index.Password)
	req.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	body, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}
	log.Println("获取最后更新时间： " + string(body) + "\r\n")
	res := Res{}
	json.Unmarshal(body, &res)
	if len(res.Hits.Hits) > 0 {
		lastT := time.Unix(res.Hits.Hits[0].Sort[0]/1000, res.Hits.Hits[0].Sort[0] % 1000 * 1000000)
		t = &lastT
	}
	return
}

type Res struct {
	Hits Hits `json:"hits"`
}

type Hits struct {
	Hits []Source `json:"hits"`
}

type Source struct {
	Id     string      `json:"_id"`
	Source InnerSource `json:"_source"`
	Sort   []int64     `json:"sort"`
}

type InnerSource struct {
	timestamp *time.Time `json:"@timestamp"`
}
