package main

import (
	"net/http"
	"fmt"
	"github.com/geekfghuang/dasava/sink"
	"encoding/json"
	"os"
)

const (
	LogUrl = "/log"
	SearchUrl = "/search"
	HttpAddr = ":12001"

	VisualUrl = "/visual"
)

type LogResp struct {
	Code int
	Msg string
}

/*
 * LogServe
 *
 * 1. （日志）数据模板，带固定message tag、其他tag多变的KV对：
 * [2018-01-14 20:20:35] [商城搜索系统] [INFO] [geekfghuang输入go并发编程实战]
 * [2018-01-14 20:35:01] [商城搜索系统] [INFO] [geekfghuang输入hadoop权威指南]
 *
 * 2. 数据经过thrift服务落入hbase
 * https://github.com/apache/thrift
 *************************************************************************/
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
		ReturnJsonObj(&LogResp{Code:-1, Msg:"日志格式错误"}, w)
		return
	}
	urlValue["client"] = []string{r.RemoteAddr}
	err := sink.Put(urlValue)
	if err != nil {
		ReturnJsonObj(&LogResp{Code:-1, Msg:err.Error()}, w)
		return
	}
	ReturnJsonObj(&LogResp{Code:200, Msg:"log成功"}, w)
}

func SearchServe(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	urlValue := r.Form
	searchParam := new(sink.SearchParam)
	if len(urlValue["client"]) > 0 {
		searchParam.Client = urlValue["client"][0]
	}
	if len(urlValue["startTime"]) > 0 {
		searchParam.StartTime = urlValue["startTime"][0]
	}
	if len(urlValue["endTime"]) > 0 {
		searchParam.EndTime = urlValue["endTime"][0]
	}
	if len(urlValue["tag"]) > 0 {
		searchParam.Tag = urlValue["tag"][0]
	}
	if len(urlValue["tagValue"]) > 0 {
		searchParam.TagValue = urlValue["tagValue"][0]
	}
	sink.Search(searchParam)
 	ReturnJsonObj(searchParam, w)
}

func ReturnJsonObj(resp interface{}, w http.ResponseWriter) {
	bytes, err := json.Marshal(resp)
	if err != nil {
		fmt.Printf("error marshal logresp: %v\n", err)
		os.Exit(1)
	}
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