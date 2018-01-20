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
	"strings"
)

const (
	HBaseHost = "140.143.146.101"
	HBasePort = "9090"
	HBaseTable = "dasava_log"
	HBaseIndexTable = "dasava_log_index"
	TimeSeq = "2006-01-02 15:04:05"

	SnowflakeUrl = "http://101.200.45.225:12009/nextId"

	Epoch         = 1516170660000
	TimeStampShift = 22

	IndexJobChSize = 100000
	BuildIndexWorkerNum = 10
)

var (
	// 关于共享对象方法的疑惑 TODO
	HBaseClient *hbase.THBaseServiceClient
	HBaseIndexClient *hbase.THBaseServiceClient

	IndexJobCh chan *IndexJob
)

type SnowflakeBody struct {
	Code int
	Msg string
	Id int64
}

type SearchParam struct {
	ClientIP string
	StartTime string
	EndTime string
	Tags string
}

type IndexJob struct {
	RowKey string
	M map[string][]string
}

type TResultString struct {
	Row string
	Time string
	TagValuesString string
	MessageString string
}

func init() {
	HBaseClient = MakeTHBaseServiceClient(HBaseHost, HBasePort)
	HBaseIndexClient = MakeTHBaseServiceClient(HBaseHost, HBasePort)
	IndexJobCh = make(chan *IndexJob, IndexJobChSize)
	for i := 0; i < BuildIndexWorkerNum; i++ {
		go BuildIndexWorker()
	}
}

func MakeTHBaseServiceClient(host, port string) (client *hbase.THBaseServiceClient) {
	transport, err := thrift.NewTSocket(net.JoinHostPort(host, port))
	if err != nil {
		fmt.Printf("error resolving address " + host + ":" + port + " :%v\n", err)
		os.Exit(1)
	}
	if err = transport.Open(); err != nil {
		fmt.Printf("error opening socket to " + host + ":" + port + " :%v\n", err)
		os.Exit(1)
	}
	client = hbase.NewTHBaseServiceClient(thrift.NewTStandardClient(thrift.NewTBinaryProtocolTransport(transport),
		thrift.NewTBinaryProtocolTransport(transport)))
	return
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
			putTColumnValues = append(putTColumnValues, &hbase.TColumnValue{Family:[]byte("message"),
				Qualifier:[]byte("message"), Value:[]byte(m["message"][0])})
		} else {
			putTColumnValues = append(putTColumnValues, &hbase.TColumnValue{Family:[]byte("tag"),
				Qualifier:[]byte(k), Value:[]byte(v[0])})
		}
	}
	rowKey := nextId()
	tPut := &hbase.TPut{Row:[]byte(rowKey), ColumnValues:putTColumnValues}
	err := HBaseClient.Put(nil, []byte(HBaseTable), tPut)
	if err != nil {
		fmt.Printf("error HBase put :%v\n", err)
	}
	IndexJobCh <- &IndexJob{RowKey:rowKey, M:m}
	return err
}

// 创建HBase二级索引
func BuildIndexWorker() {
	for indexJob := range IndexJobCh {
		// 拼接log表rowKey在最前面
		// 1. index表rowKey唯一
		// 2. 时间相近的尽可能在同一region，减少网络IO
		indexRowKey := indexJob.RowKey
		for k, v := range indexJob.M {
			if k == "message" {
				continue
			}
			indexRowKey += k + v[0]
		}
		indexTColumnValues := make([]*hbase.TColumnValue, 0, 5)
		indexTColumnValues = append(indexTColumnValues, &hbase.TColumnValue{Family: []byte("index"),
			Qualifier: []byte("rowKey"), Value: []byte(indexJob.RowKey)})
		indexTPut := &hbase.TPut{Row: []byte(indexRowKey),
			ColumnValues: indexTColumnValues}
		err := HBaseIndexClient.Put(nil, []byte(HBaseIndexTable), indexTPut)
		if err != nil {
			fmt.Printf("error HBase index put :%v\n", err)
		}
	}
}

func Search(searchParam *SearchParam) []*TResultString {
	tScan := hbase.NewTScan()
	var tResults []*hbase.TResult_
	if searchParam.ClientIP == "" && searchParam.Tags == "" {
		// 没有索引的起止时间查询：
		// 将yyyy-MM-dd HH:mm:ss格式的数据变换成HBase rowKey范围，由于put数据时rowKey来自其他
		// 网络节点上的snowflake服务，而HBase cell的时间为数据真正落地的时间，所以会有一些误差。
		// 网络上的延迟、抖动都会影响到查询误差。
		if searchParam.StartTime != "" {
			startTime, _ := time.Parse(TimeSeq, searchParam.StartTime)
			startRow := ((startTime.Unix() - 8 * 3600) * 1000 - Epoch) << TimeStampShift
			tScan.StartRow = []byte(strconv.FormatInt(startRow, 10))
		}
		if searchParam.EndTime != "" {
			endTime, _ := time.Parse(TimeSeq, searchParam.EndTime)
			stopRow := ((endTime.Unix() - 8 * 3600 + 1) * 1000 - Epoch) << TimeStampShift
			tScan.StopRow = []byte(strconv.FormatInt(stopRow, 10))
		}
		scannerID, err := HBaseClient.OpenScanner(nil, []byte(HBaseTable), tScan)
		if err != nil {
			fmt.Printf("error openScanner: %v\n", err)
			os.Exit(1)
		}
		tResults, err = HBaseClient.GetScannerRows(nil, scannerID, 100)
		if err != nil {
			fmt.Printf("error getScannerRows: %v\n", err)
			os.Exit(1)
		}
	} else {
		// HBase二级索引查询
		// 先查index表得到一系列log表的rowKey，再getMultiple
		var filterString string
		if searchParam.ClientIP != "" {
			filterString += "RowFilter(=,'substring:clientIP" + searchParam.ClientIP + "') AND "
		}
		if searchParam.Tags != "" {
			kvs := strings.Split(searchParam.Tags[1:len(searchParam.Tags)-1], ",")
			for _, kv := range kvs {
				var rowFilterTarget string
				kvSplit := strings.Split(kv, `":"`)
				rowFilterTarget += kvSplit[0][1:]
				if len(kvSplit) > 1 {
					rowFilterTarget += kvSplit[1][:len(kvSplit[1])-1]
				}
				filterString += "RowFilter(=,'substring:" + rowFilterTarget + "') AND "
			}
		}
		filterString = filterString[:len(filterString)-5]
		tScan.FilterString = []byte(filterString)
		scannerID, err := HBaseClient.OpenScanner(nil, []byte(HBaseIndexTable), tScan)
		if err != nil {
			fmt.Printf("error openScanner: %v\n", err)
			os.Exit(1)
		}
		tResultIndexs, err := HBaseClient.GetScannerRows(nil, scannerID, 100)
		if err != nil {
			fmt.Printf("error getScannerRows: %v\n", err)
			os.Exit(1)
		}
		tGets := make([]*hbase.TGet, 0, 100)
		leftThreshold, rightThreshold := int64(-1), int64(-1)
		if searchParam.StartTime != "" {
			startTime, _ := time.Parse(TimeSeq, searchParam.StartTime)
			leftThreshold= ((startTime.Unix() - 8 * 3600) * 1000 - Epoch) << TimeStampShift
		}
		if searchParam.EndTime != "" {
			endTime, _ := time.Parse(TimeSeq, searchParam.EndTime)
			rightThreshold = ((endTime.Unix() - 8 * 3600 + 1) * 1000 - Epoch) << TimeStampShift
		}
		for _, v := range tResultIndexs {
			rowKey, _ := strconv.ParseInt(string(v.GetColumnValues()[0].Value), 10, 64)
			if leftThreshold != int64(-1) {
				if rowKey < leftThreshold {
					continue
				}
			}
			if rightThreshold != int64(-1) {
				if rowKey > rightThreshold {
					continue
				}
			}
			tGets = append(tGets, &hbase.TGet{Row:[]byte(strconv.FormatInt(rowKey, 10))})
		}
		tResults, err = HBaseClient.GetMultiple(nil, []byte(HBaseTable), tGets)
		if err != nil {
			fmt.Printf("error getMultiple: %v\n", err)
			os.Exit(1)
		}
	}
	return StrTResults(tResults)
}

func StrTResults(tResults []*hbase.TResult_) []*TResultString {
	tResultStrings := make([]*TResultString, 0, 100)
	for _, v := range tResults {
		tResultString := new(TResultString)
		tResultString.Row = string(v.Row)
		for _, tColumnValue := range v.GetColumnValues() {
			if string(tColumnValue.Family) == "message" && string(tColumnValue.Qualifier) == "message" {
				tResultString.MessageString = string(tColumnValue.Value)
			} else {
				tResultString.TagValuesString += string(tColumnValue.Qualifier) + ":" + string(tColumnValue.Value) + ","
			}
		}
		tResultString.TagValuesString = tResultString.TagValuesString[:len(tResultString.TagValuesString)-1]
		time64, _ := strconv.ParseInt(string(v.Row), 10, 64)
		tResultString.Time = time.Unix(((time64 >> TimeStampShift) + Epoch) / 1000, 0).Format(TimeSeq)
		tResultStrings = append(tResultStrings, tResultString)
	}
	return tResultStrings
}