package db

import (
	"fmt"
	"testing"
)

func TestMysql(t *testing.T) {
	InitDB()
	nodes := QueryChunkOnDataNode("127.0.0.1:8082")
	fmt.Println(nodes)
	chunks := QueryChunks(nodes)
	fmt.Println(chunks)
}
