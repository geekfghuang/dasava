package sink

import (
	"hbase"
	"git.apache.org/thrift.git/lib/go/thrift"
	"net"
	"fmt"
	"os"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"strconv"
)

const (
	HBaseHost = "140.143.146.101"
	HBasePort = "9090"
	HBaseTable = "dasava_log"
	TimeSeq = "2006-01-02 15:04:05"

	SnowflakeUrl = "http://101.200.45.225:12009/nextId"
)

var HBaseClient *hbase.THBaseServiceClient

type SnowflakeBody struct {
	Code int
	Msg string
	Id int64
}

func init() {
	transport, err := thrift.NewTSocket(net.JoinHostPort(HBaseHost, HBasePort))
	if err != nil {
		fmt.Printf("error resolving address " + HBaseHost + ":" + HBasePort + " :%v\n", err)
		os.Exit(1)
	}
	if err = transport.Open(); err != nil {
		fmt.Printf("error opening socket to " + HBaseHost + ":" + HBasePort + " :%v\n", err)
		os.Exit(1)
	}
	HBaseClient = hbase.NewTHBaseServiceClient(thrift.NewTStandardClient(thrift.NewTBinaryProtocolTransport(transport),
		thrift.NewTBinaryProtocolTransport(transport)))
}

func nextId() (uid string) {
	resp, err := http.Get(SnowflakeUrl)
	if err != nil {
		fmt.Printf("error get snowflake: %v\n", err)
		os.Exit(1)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("error read snowflake body: %v\n", err)
		os.Exit(1)
	}
	var snowflakeBody SnowflakeBody
	err = json.Unmarshal(body, &snowflakeBody)
	if err != nil {
		fmt.Printf("error unmarshal body: %v\n", err)
		os.Exit(1)
	}
	return strconv.FormatInt(snowflakeBody.Id, 10)
}

func Put(m map[string][]string) {
	putTColumnValues := make([]*hbase.TColumnValue, 0, 10)
	for k, v := range m {
		if k == "message" {
			putTColumnValues = append(putTColumnValues, &hbase.TColumnValue{Family:[]byte("message"), Qualifier:[]byte("message"), Value:[]byte(m["message"][0])})
		} else {
			putTColumnValues = append(putTColumnValues, &hbase.TColumnValue{Family:[]byte("tag"), Qualifier:[]byte(k), Value:[]byte(v[0])})
		}
	}
	tPut := &hbase.TPut{Row:[]byte(nextId()), ColumnValues:putTColumnValues}
	err := HBaseClient.Put(nil, []byte(HBaseTable), tPut)
	if err != nil {
		fmt.Printf("error hbase put :%v\n", err)
		os.Exit(1)
	}
}