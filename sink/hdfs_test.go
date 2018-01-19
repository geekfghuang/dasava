package sink

import (
	"hbase"
	"fmt"
	"os"
	"time"
	"testing"
)

// 测试时程序所有error全部忽略，直接打出控制台

func Test_Get(t *testing.T) {
	//tResult, err := HBaseClient.Get(nil, []byte(HBaseTable), &hbase.TGet{Row:[]byte("0003"), FilterString:[]byte("ValueFilter(=,'substring:geek')")})
	tResult, _ := HBaseClient.Get(nil, []byte(HBaseTable), &hbase.TGet{Row:[]byte("344405177270272")})
	tColumnValues := tResult.GetColumnValues()
	for _, tColumnValue := range tColumnValues {
		fmt.Println(string(tColumnValue.Family))
		fmt.Println(string(tColumnValue.Qualifier))
		fmt.Println(string(tColumnValue.Value))
		fmt.Println(time.Unix(*tColumnValue.Timestamp / 1000, 0).Format(TimeSeq))
	}
}

func Test_Scan(t *testing.T) {
	tScan := hbase.NewTScan()
	//ColumnPrefixFilter('c2') AND
	//tScan.FilterString = []byte("ValueFilter(=,'substring:hadoop')")
	//tc := &hbase.TColumn{Family:[]byte("message"), Qualifier:[]byte("message")}
	////tc2 := &hbase.TColumn{Family:[]byte("tag"), Qualifier:[]byte("")}
	//slice := make([]*hbase.TColumn, 0, 0)
	//slice = append(slice, tc)
	//tScan.Columns = slice

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

func Test_nextId(t *testing.T) {
	uid := nextId()
	fmt.Println(uid)
}