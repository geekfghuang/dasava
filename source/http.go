package main

import (
	"net/http"
	"fmt"
	"github.com/geekfghuang/dasava/sink"
	"encoding/json"
	"strings"
)

const (
	LogUrl = "/log"
	SearchUrl = "/search"
	HttpAddr = ":12001"

	VisualUrl = "/visual"
)

type Resp struct {
	Code int
	Msg string
	Body interface{}
}

// 1. （日志）数据模板，带固定message tag、其他tag多变的KV对
// 2. 数据经过thrift服务落入HBase，https://github.com/apache/thrift
func LogServe(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	urlValue, flag := r.Form, 0
	for k, _ := range urlValue {
		if k == "message" {
			flag = 1
			break
		}
	}
	if flag != 1 {
		WriteJsonObj(&Resp{Code:-1, Msg:"日志格式错误"}, w)
		return
	}
	urlValue["clientIP"] = []string{r.RemoteAddr[:strings.LastIndex(r.RemoteAddr, ":")]}
	err := sink.Put(urlValue)
	if err != nil {
		WriteJsonObj(&Resp{Code:-1, Msg:err.Error()}, w)
		return
	}
	WriteJsonObj(&Resp{Code:200, Msg:"log成功"}, w)
}

func SearchServe(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	urlValue := r.Form
	searchParam := new(sink.SearchParam)
	if len(urlValue["clientIP"]) > 0 {
		searchParam.ClientIP = urlValue["clientIP"][0]
	}
	if len(urlValue["startTime"]) > 0 {
		searchParam.StartTime = urlValue["startTime"][0]
	}
	if len(urlValue["endTime"]) > 0 {
		searchParam.EndTime = urlValue["endTime"][0]
	}
	if len(urlValue["tags"]) > 0 {
		searchParam.Tags = urlValue["tags"][0]
	}
	tResultStrings, err := sink.Search(searchParam)
	if err != nil {
		WriteJsonObj(&Resp{Code:200, Msg:"查询失败", Body:err.Error()}, w)
		return
	}
	WriteJsonObj(&Resp{Code:200, Msg:"查询成功", Body:tResultStrings}, w)
}

func WriteJsonObj(resp interface{}, w http.ResponseWriter) {
	bytes, _ := json.Marshal(resp)
	w.Header().Set("Content-Type","application/json")
	w.Write(bytes)
}

func main() {
	// 以http方式接入log数据
	http.HandleFunc(LogUrl, LogServe)
	// log数据搜索入口
	http.HandleFunc(SearchUrl, SearchServe)
	// 可视化界面
	http.Handle(VisualUrl, http.StripPrefix(VisualUrl, http.FileServer(http.Dir("C:\\Users\\huangfugui\\GoglandProjects\\src\\github.com\\geekfghuang\\dasava\\webapps\\log\\"))))
	err := http.ListenAndServe(HttpAddr, nil)
	if err != nil {
		fmt.Printf("error ListenAndServe: %v\n", err)
	}
}