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
	transport, err := thrift.NewTSocket(net.JoinHostPort("140.143.146.101", "9090"))
	if err != nil {
		fmt.Errorf("error resolving address 140.143.146.101:9090 :%v", err)
		os.Exit(1)
	}
	if err = transport.Open(); err != nil {
		fmt.Errorf("error opening socket to 140.143.146.101:9090 :%v", err)
		os.Exit(1)
	}
	defer transport.Close()
	client := hbase.NewTHBaseServiceClient(thrift.NewTStandardClient(thrift.NewTBinaryProtocolTransport(transport),
		thrift.NewTBinaryProtocolTransport(transport)))

	tResult, err := client.Get(nil, []byte("logtable"), &hbase.TGet{Row:[]byte("0003")})
	if err != nil {
		fmt.Errorf("error Get :%v", err)
	}
	tColumnValues := tResult.GetColumnValues()
	for _, tColumnValue := range tColumnValues {
		fmt.Println(string(tColumnValue.Family))
		fmt.Println(string(tColumnValue.Qualifier))
		fmt.Println(string(tColumnValue.Value))
		fmt.Println(time.Unix(*tColumnValue.Timestamp, 0).Format("2006-01-02 15:04:05"))
	}

	//putTColumnValues := make([]*hbase.TColumnValue, 0, 10)
	//putTColumnValues = append(putTColumnValues, &hbase.TColumnValue{Family:[]byte("msg"), Qualifier:[]byte("username"), Value:[]byte("geekfghuang")})
	//putTColumnValues = append(putTColumnValues, &hbase.TColumnValue{Family:[]byte("msg"), Qualifier:[]byte("title"), Value:[]byte("高级软件开发工程师")})
	//tPut := &hbase.TPut{Row:[]byte("0003"), ColumnValues:putTColumnValues}
	//err = client.Put(nil, []byte("logtable"), tPut)
	//if err != nil {
	//	fmt.Errorf("error Put :%v", err)
	//}
}

func main() {
	// 以http方式接入数据
	http.HandleFunc("/log", logCollector)
	err := http.ListenAndServe(":12001", nil)
	if err != nil {
		fmt.Errorf("%v\n", err)
	}
}