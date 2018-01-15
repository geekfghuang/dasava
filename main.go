package main

import (
	"net/http"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"net"
	"os"
	"hbase"
	"time"
)

const (
	HttpUrl = "/log"
	HttpAddr = ":12001"

	Host = "140.143.146.101"
	Port = "9090"
	Table = "logtable"
	TimeSeq = "2006-01-02 15:04:05"
)

/*
 * 1. （日志）数据模板，KV对：
 * [2018-01-14 20:20:35] [商城搜索系统] [INFO] [geekfghuang输入go并发编程实战]
 * [2018-01-14 20:35:01] [商城搜索系统] [INFO] [geekfghuang输入hadoop权威指南]
 *
 * 2. 数据经过thrift服务落入hbase
 * https://github.com/apache/thrift
 *************************************************************************/
func logCollector(w http.ResponseWriter, r *http.Request) {
	//clientIP := r.Host
	r.ParseForm()
	m := r.Form
	for k, v := range m {
		fmt.Println("key " + k)
		fmt.Println("value " + v[0])
	}
	transport, err := thrift.NewTSocket(net.JoinHostPort(Host, Port))
	if err != nil {
		fmt.Printf("error resolving address " + Host + ":" + Port + " :%v\n", err)
		os.Exit(1)
	}
	if err = transport.Open(); err != nil {
		fmt.Printf("error opening socket to " + Host + ":" + Port + " :%v\n", err)
		os.Exit(1)
	}
	defer transport.Close()
	client := hbase.NewTHBaseServiceClient(thrift.NewTStandardClient(thrift.NewTBinaryProtocolTransport(transport),
		thrift.NewTBinaryProtocolTransport(transport)))

	tResult, err := client.Get(nil, []byte(Table), &hbase.TGet{Row:[]byte("0003")})
	if err != nil {
		fmt.Printf("error Get :%v\n", err)
	}
	tColumnValues := tResult.GetColumnValues()
	for _, tColumnValue := range tColumnValues {
		fmt.Println(string(tColumnValue.Family))
		fmt.Println(string(tColumnValue.Qualifier))
		fmt.Println(string(tColumnValue.Value))
		fmt.Println(time.Unix(*tColumnValue.Timestamp, 0).Format(TimeSeq))
	}

	putTColumnValues := make([]*hbase.TColumnValue, 0, 10)
	putTColumnValues = append(putTColumnValues, &hbase.TColumnValue{Family:[]byte("msg"), Qualifier:[]byte("username"), Value:[]byte("geekfghuang")})
	putTColumnValues = append(putTColumnValues, &hbase.TColumnValue{Family:[]byte("msg"), Qualifier:[]byte("title"), Value:[]byte("高级软件开发工程师")})
	tPut := &hbase.TPut{Row:[]byte("0003"), ColumnValues:putTColumnValues}
	err = client.Put(nil, []byte(Table), tPut)
	if err != nil {
		fmt.Printf("error Put :%v\n", err)
	}
}

func main() {
	// 以http方式接入数据
	http.HandleFunc(HttpUrl, logCollector)
	err := http.ListenAndServe(HttpAddr, nil)
	if err != nil {
		fmt.Printf("error ListenAndServe: %v\n", err)
	}
}