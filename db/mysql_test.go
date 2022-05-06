package db

import (
	"encoding/hex"
	"fmt"
	"mini-dfs/dfs"
	"testing"
)

func TestMysql(t *testing.T) {
	encode := dfs.MD5Encode([]byte("TEST"))
	fmt.Println(hex.EncodeToString(encode))
	//InitDB()
	//fileid := InsertFile("test")
	//fmt.Println(fileid)
}
