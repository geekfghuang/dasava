package main

import (
	"net/http"
	"fmt"
	"github.com/geekfghuang/dasava/sink"
)

const (
	HttpUrl = "/log"
	HttpAddr = ":12001"
)

/*
 * serveHTTP
 *
 * 1. （日志）数据模板，带固定message tag、其他tag多变的KV对：
 * [2018-01-14 20:20:35] [商城搜索系统] [INFO] [geekfghuang输入go并发编程实战]
 * [2018-01-14 20:35:01] [商城搜索系统] [INFO] [geekfghuang输入hadoop权威指南]
 *
 * 2. 数据经过thrift服务落入hbase
 * https://github.com/apache/thrift
 *************************************************************************/
func serveHTTP(w http.ResponseWriter, r *http.Request) {
	//clientIP := r.Host
	r.ParseForm()
	urlValue, flag := r.Form, 0
	for k, _ := range urlValue {
		if k == "message" {
			flag = 1
			break
		}
	}
	if flag != 1 {
		return
	}
	sink.Put(urlValue)
}

func main() {
	// 以http方式接入数据
	http.HandleFunc(HttpUrl, serveHTTP)
	err := http.ListenAndServe(HttpAddr, nil)
	if err != nil {
		fmt.Printf("error ListenAndServe: %v\n", err)
	}
}