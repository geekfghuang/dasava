package main

import (
	"net/http"
	"fmt"
	"github.com/geekfghuang/dasava/sink"
	"encoding/json"
	"os"
)

const (
	HttpUrl = "/log"
	HttpAddr = ":12001"
)

type LogResp struct {
	Code int
	Msg string
}

/*
 * ServeHTTP
 *
 * 1. （日志）数据模板，带固定message tag、其他tag多变的KV对：
 * [2018-01-14 20:20:35] [商城搜索系统] [INFO] [geekfghuang输入go并发编程实战]
 * [2018-01-14 20:35:01] [商城搜索系统] [INFO] [geekfghuang输入hadoop权威指南]
 *
 * 2. 数据经过thrift服务落入hbase
 * https://github.com/apache/thrift
 *************************************************************************/
func ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	err := sink.Put(urlValue)
	if err != nil {
		ReturnJsonObj(&LogResp{Code:-1, Msg:err.Error()}, w)
		return
	}
	ReturnJsonObj(&LogResp{Code:200, Msg:"log成功"}, w)
}

func ReturnJsonObj(resp *LogResp, w http.ResponseWriter) {
	bytes, err := json.Marshal(resp)
	if err != nil {
		fmt.Printf("error marshal logresp: %v\n", err)
		os.Exit(1)
	}
	w.Header().Set("Content-Type","application/json")
	w.Write(bytes)
}

func main() {
	// 以http方式接入数据
	http.HandleFunc(HttpUrl, ServeHTTP)
	err := http.ListenAndServe(HttpAddr, nil)
	if err != nil {
		fmt.Printf("error ListenAndServe: %v\n", err)
	}
}