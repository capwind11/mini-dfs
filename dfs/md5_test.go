package dfs

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func TestMysql(t *testing.T) {
	encode := MD5Encode([]byte("TESTetwrgewgagfd"))
	fmt.Println(hex.EncodeToString(encode))
	encode1 := MD5Encode([]byte("TESTet"))
	fmt.Println(hex.EncodeToString(encode1))
	//InitDB()
	//fileid := InsertFile("test")
	//fmt.Println(fileid)
}
