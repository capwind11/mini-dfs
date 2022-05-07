package dfs

import (
	"crypto/md5"
	"log"
	"os"
)

var ns_logger = log.New(os.Stdout, "NameServer:", log.Lshortfile)
var ds_logger = log.New(os.Stdout, "DataNode:", log.Lshortfile)
var client_logger = log.New(os.Stdout, "client:", log.Lshortfile)

func MD5Encode(chunk []byte) []byte {
	h := md5.New()
	h.Write(chunk)
	return h.Sum(nil)
}

func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}
