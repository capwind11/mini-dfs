package dfs

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func TestMysql(t *testing.T) {
	encode := MD5Encode([]byte("wrgewgagfd"))
	fmt.Println(hex.EncodeToString(encode))
	encode1 := MD5Encode([]byte("dsafdgaadfg"))
	fmt.Println(hex.EncodeToString(encode1))
}
