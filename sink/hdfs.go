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
	"time"
)

const (
	HBaseHost = "140.143.146.101"
	HBasePort = "9090"
	HBaseTable = "dasava_log"
	TimeSeq = "2006-01-02 15:04:05"

	SnowflakeUrl = "http://101.200.45.225:12009/nextId"

	Epoch         = 1516170660000
	TimeStampShift = 22
)

var HBaseClient *hbase.THBaseServiceClient

type SnowflakeBody struct {
	Code int
	Msg string
	Id int64
}

type SearchParam struct {
	Client string
	StartTime string
	EndTime string
	Tag string
	TagValue string
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

func Put(m map[string][]string) error {
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
	}
	return err
}

func Search(searchParam *SearchParam) {
	tScan := hbase.NewTScan()

	// 起止时间查询：
	// 将yyyy-MM-dd HH:mm:ss格式的数据变换成hbase rowkey范围，
	// 由于put数据时rowkey来自其他网络节点上的snowflake服务，而
	// hbase cell的时间为数据真正落地的时间，所以会有一些误差。
	// 网络上的延迟、抖动都会影响到查询误差。
	if searchParam.StartTime != "" {
		startTime, _ := time.Parse(TimeSeq, searchParam.StartTime)
		startRow := ((startTime.Unix() - 8 * 3600) * 1000 - Epoch) << TimeStampShift
		tScan.StartRow = []byte(strconv.FormatInt(startRow, 10))
	}
	if searchParam.EndTime != "" {
		endTime, _ := time.Parse(TimeSeq, searchParam.EndTime)
		stopRow := ((endTime.Unix() - 8 * 3600) * 1000 - Epoch) << TimeStampShift
		tScan.StopRow = []byte(strconv.FormatInt(stopRow, 10))
	}
	//tScan.FilterString = []byte("ValueFilter(=,'substring:分布式')")
	scannerID, err := HBaseClient.OpenScanner(nil, []byte(HBaseTable), tScan)
	if err != nil {
		fmt.Printf("error openScanner: %v\n", err)
		os.Exit(1)
	}
	tResultSlice, err := HBaseClient.GetScannerRows(nil, scannerID, 100)
	if err != nil {
		fmt.Printf("error getScannerRows: %v\n", err)
		os.Exit(1)
	}
	for _, v := range tResultSlice {
		tColumnValues := v.GetColumnValues()
		fmt.Println("-----------------------------------------------------------------")
		for _, tColumnValue := range tColumnValues {
			fmt.Println(string(tColumnValue.Family))
			fmt.Println(string(tColumnValue.Qualifier))
			fmt.Println(string(tColumnValue.Value))
			fmt.Println(time.Unix(*tColumnValue.Timestamp / 1000, 0).Format(TimeSeq))
			fmt.Println()
		}
	}
}